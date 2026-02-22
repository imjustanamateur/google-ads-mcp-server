"""Conversion tracking tools for Google Ads MCP Server."""
import logging
import requests
from typing import Any, Dict, List
from fastmcp import Context
from mcp_instance import mcp
from oauth.google_auth import (
    format_customer_id, get_headers_with_auto_token,
    execute_gaql, API_VERSION, GOOGLE_ADS_DEVELOPER_TOKEN,
    _make_request,
)

logger = logging.getLogger(__name__)


@mcp.tool
def list_conversion_actions(
    customer_id: str,
    include_removed: bool = False,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all conversion actions (goals) configured for the account."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching conversion actions for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clause = "" if include_removed else "WHERE conversion_action.status != 'REMOVED'"

        query = f"""
            SELECT
                conversion_action.id,
                conversion_action.name,
                conversion_action.status,
                conversion_action.type,
                conversion_action.category,
                conversion_action.counting_type,
                conversion_action.value_settings.default_value,
                conversion_action.value_settings.always_use_default_value,
                conversion_action.click_through_lookback_window_days,
                conversion_action.view_through_lookback_window_days,
                conversion_action.include_in_conversions_metric
            FROM conversion_action
            {where_clause}
            ORDER BY conversion_action.name ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        conversions = []
        for row in rows:
            ca = row.get("conversionAction", {})
            vs = ca.get("valueSettings", {})
            conversions.append({
                "id": str(ca.get("id", "")),
                "name": ca.get("name", ""),
                "status": ca.get("status", ""),
                "type": ca.get("type", ""),
                "category": ca.get("category", ""),
                "counting_type": ca.get("countingType", ""),
                "default_value": vs.get("defaultValue"),
                "always_use_default_value": vs.get("alwaysUseDefaultValue", False),
                "click_through_lookback_days": ca.get("clickThroughLookbackWindowDays"),
                "view_through_lookback_days": ca.get("viewThroughLookbackWindowDays"),
                "include_in_conversions_metric": ca.get("includeInConversionsMetric", True),
            })

        if ctx:
            ctx.info(f"Found {len(conversions)} conversion actions.")

        return {
            "conversion_actions": conversions,
            "total": len(conversions),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_conversion_actions failed: {str(e)}")
        raise


@mcp.tool
def get_conversion_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get conversion performance broken down by conversion action."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()

    if ctx:
        ctx.info(f"Fetching conversion performance for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            f"segments.date DURING {date_range}",
            "campaign.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                segments.conversion_action,
                segments.conversion_action_name,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.cost_micros
            FROM campaign
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.conversions DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        data = []
        for row in rows:
            m = row.get("metrics", {})
            seg = row.get("segments", {})
            camp = row.get("campaign", {})
            cost_micros = int(m.get("costMicros", 0))

            data.append({
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "conversion_action": seg.get("conversionAction", ""),
                "conversion_action_name": seg.get("conversionActionName", ""),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "all_conversions": float(m.get("allConversions", 0)),
                "all_conversions_value": float(m.get("allConversionsValue", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
            })

        if ctx:
            ctx.info(f"Retrieved {len(data)} conversion rows.")

        return {
            "conversions": data,
            "total": len(data),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_conversion_performance failed: {str(e)}")
        raise


@mcp.tool
def create_conversion_action(
    customer_id: str,
    name: str,
    category: str = "DEFAULT",
    counting_type: str = "ONE_PER_CLICK",
    default_value: float = 0.0,
    click_through_lookback_days: int = 30,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a new conversion action (goal). category options: DEFAULT, PAGE_VIEW, PURCHASE, SIGNUP, LEAD, DOWNLOAD. counting_type: ONE_PER_CLICK or MANY_PER_CLICK."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating conversion action '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/conversionActions:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "name": name,
                        "category": category,
                        "type": "WEBPAGE",
                        "status": "ENABLED",
                        "countingType": counting_type,
                        "valueSettings": {
                            "defaultValue": default_value,
                            "alwaysUseDefaultValue": default_value > 0,
                        },
                        "clickThroughLookbackWindowDays": click_through_lookback_days,
                    }
                }
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [{}])
        resource_name = results[0].get("resourceName", "") if results else ""

        if ctx:
            ctx.info(f"Conversion action created: {resource_name}")

        return {
            "conversion_action_created": resource_name,
            "name": name,
            "category": category,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_conversion_action failed: {str(e)}")
        raise


@mcp.tool
def update_conversion_action(
    customer_id: str,
    conversion_action_id: str,
    name: str = "",
    default_value: float = -1.0,
    click_through_lookback_days: int = -1,
    status: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Update an existing conversion action's settings. Pass only the fields you want to change."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Updating conversion action {conversion_action_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/conversionActions:mutate"

        update_body: Dict[str, Any] = {
            "resourceName": f"customers/{cid}/conversionActions/{conversion_action_id}"
        }
        update_mask_fields = []

        if name:
            update_body["name"] = name
            update_mask_fields.append("name")
        if status:
            update_body["status"] = status
            update_mask_fields.append("status")
        if click_through_lookback_days >= 0:
            update_body["clickThroughLookbackWindowDays"] = click_through_lookback_days
            update_mask_fields.append("click_through_lookback_window_days")
        if default_value >= 0:
            update_body["valueSettings"] = {"defaultValue": default_value}
            update_mask_fields.append("value_settings.default_value")

        if not update_mask_fields:
            return {"message": "No fields to update provided.", "customer_id": customer_id}

        body = {
            "operations": [
                {
                    "update": update_body,
                    "updateMask": ",".join(update_mask_fields),
                }
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Conversion action {conversion_action_id} updated successfully.")

        return {
            "conversion_action_id": conversion_action_id,
            "updated_fields": update_mask_fields,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"update_conversion_action failed: {str(e)}")
        raise
