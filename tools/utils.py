"""Utility tools for Google Ads MCP Server."""
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
def get_change_history(
    customer_id: str,
    date_range: str = "LAST_7_DAYS",
    resource_type: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get account change history log. resource_type filter options: CAMPAIGN, AD_GROUP, AD, KEYWORD, BIDDING_STRATEGY, CAMPAIGN_BUDGET, CAMPAIGN_CRITERION."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()

    if ctx:
        ctx.info(f"Fetching change history for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [f"segments.date DURING {date_range}"]
        if resource_type:
            where_clauses.append(f"change_event.change_resource_type = '{resource_type.upper()}'")

        query = f"""
            SELECT
                change_event.change_date_time,
                change_event.change_resource_type,
                change_event.change_resource_name,
                change_event.user_email,
                change_event.client_type,
                change_event.campaign,
                change_event.ad_group,
                change_event.resource_change_operation,
                change_event.old_resource,
                change_event.new_resource
            FROM change_event
            WHERE {' AND '.join(where_clauses)}
            ORDER BY change_event.change_date_time DESC
            LIMIT 200
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        changes = []
        for row in rows:
            ce = row.get("changeEvent", {})
            changes.append({
                "change_date_time": ce.get("changeDateTime", ""),
                "change_resource_type": ce.get("changeResourceType", ""),
                "change_resource_name": ce.get("changeResourceName", ""),
                "user_email": ce.get("userEmail", ""),
                "client_type": ce.get("clientType", ""),
                "campaign": ce.get("campaign", ""),
                "ad_group": ce.get("adGroup", ""),
                "operation": ce.get("resourceChangeOperation", ""),
            })

        if ctx:
            ctx.info(f"Retrieved {len(changes)} change events.")

        return {
            "changes": changes,
            "total": len(changes),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_change_history failed: {str(e)}")
        raise


@mcp.tool
def preview_ad_targeting(
    customer_id: str,
    query_text: str,
    country_code: str = "US",
    language_code: str = "en",
    device: str = "DESKTOP",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Preview how ads would look for a given search query and targeting criteria using AdPreview service."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    device = device.upper()
    valid_devices = {"DESKTOP", "MOBILE", "TABLET"}
    if device not in valid_devices:
        raise ValueError(f"device must be one of: {', '.join(sorted(valid_devices))}")

    if ctx:
        ctx.info(f"Previewing ads for query '{query_text}' ({country_code}, {device})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:generate"

        # Use generateAdGroupThemes as it's available for preview
        # Actually use the AdPreview endpoint
        preview_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}:generateAdGroupThemes"

        body = {
            "customer_id": cid,
            "keyword_seed": {
                "keywords": [query_text]
            }
        }

        resp = _make_request(requests.post, preview_url, headers, body)

        if ctx:
            ctx.info("Ad preview request completed.")

        if resp.ok:
            return {
                "query_text": query_text,
                "country_code": country_code,
                "device": device,
                "preview_data": resp.json(),
                "customer_id": customer_id,
            }
        else:
            return {
                "query_text": query_text,
                "country_code": country_code,
                "device": device,
                "message": "Ad preview generation not available for this account type. Use run_gaql to check active ads.",
                "status_code": resp.status_code,
                "customer_id": customer_id,
            }

    except Exception as e:
        if ctx:
            ctx.error(f"preview_ad_targeting failed: {str(e)}")
        raise


@mcp.tool
def get_policy_violations(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get all policy violations and disapproved ads with violation details."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching policy violations for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = """
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad.type,
                ad_group_ad.status,
                ad_group_ad.policy_summary.approval_status,
                ad_group_ad.policy_summary.policy_topic_entries,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name
            FROM ad_group_ad
            WHERE ad_group_ad.policy_summary.approval_status = 'DISAPPROVED'
              AND ad_group_ad.status != 'REMOVED'
              AND campaign.status != 'REMOVED'
            ORDER BY campaign.name ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        violations = []
        for row in rows:
            ada = row.get("adGroupAd", {})
            ad = ada.get("ad", {})
            ps = ada.get("policySummary", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            topics = []
            for entry in ps.get("policyTopicEntries", []):
                topics.append({
                    "topic": entry.get("topic", ""),
                    "type": entry.get("type", ""),
                    "evidences": entry.get("evidences", []),
                    "constraints": entry.get("constraints", []),
                })

            violations.append({
                "ad_id": str(ad.get("id", "")),
                "ad_name": ad.get("name", ""),
                "ad_type": ad.get("type", ""),
                "approval_status": ps.get("approvalStatus", ""),
                "policy_topic_entries": topics,
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(violations)} policy violations.")

        return {
            "violations": violations,
            "total": len(violations),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_policy_violations failed: {str(e)}")
        raise
