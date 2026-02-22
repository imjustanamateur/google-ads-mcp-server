"""Budget and bidding management tools for Google Ads MCP Server."""
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
def move_keywords(
    customer_id: str,
    keyword_criterion_ids: List[str],
    source_ad_group_id: str,
    destination_ad_group_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Move keywords from one ad group to another by removing and re-creating them."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Moving {len(keyword_criterion_ids)} keyword(s) from ad group {source_ad_group_id} to {destination_ad_group_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        # First, fetch the keyword details we need to recreate them
        criterion_ids_str = ", ".join(keyword_criterion_ids)
        query = f"""
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.cpc_bid_micros,
                ad_group_criterion.final_urls,
                ad_group_criterion.status
            FROM ad_group_criterion
            WHERE ad_group.id = {source_ad_group_id}
              AND ad_group_criterion.criterion_id IN ({criterion_ids_str})
              AND ad_group_criterion.type = 'KEYWORD'
        """

        result = execute_gaql(cid, query, mgr)
        keywords_data = result.get("results", [])

        if not keywords_data:
            return {"error": "No matching keywords found in source ad group.", "customer_id": customer_id}

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupCriteria:mutate"

        # Remove from source
        remove_ops = [
            {"remove": f"customers/{cid}/adGroupCriteria/{source_ad_group_id}~{crit_id}"}
            for crit_id in keyword_criterion_ids
        ]
        remove_body = {"operations": remove_ops}
        remove_resp = _make_request(requests.post, url, headers, remove_body)
        if not remove_resp.ok:
            raise Exception(f"Error removing keywords: {remove_resp.status_code} {remove_resp.text}")

        # Create in destination
        create_ops = []
        for row in keywords_data:
            crit = row.get("adGroupCriterion", {})
            kw = crit.get("keyword", {})
            create_op: Dict[str, Any] = {
                "create": {
                    "adGroup": f"customers/{cid}/adGroups/{destination_ad_group_id}",
                    "keyword": {
                        "text": kw.get("text", ""),
                        "matchType": kw.get("matchType", ""),
                    },
                    "status": crit.get("status", "ENABLED"),
                }
            }
            bid_micros = int(crit.get("cpcBidMicros", 0))
            if bid_micros:
                create_op["create"]["cpcBidMicros"] = str(bid_micros)
            create_ops.append(create_op)

        create_body = {"operations": create_ops}
        create_resp = _make_request(requests.post, url, headers, create_body)
        if not create_resp.ok:
            raise Exception(f"Error creating keywords: {create_resp.status_code} {create_resp.text}")

        if ctx:
            ctx.info(f"Moved {len(keyword_criterion_ids)} keywords successfully.")

        return {
            "keywords_moved": len(keyword_criterion_ids),
            "source_ad_group_id": source_ad_group_id,
            "destination_ad_group_id": destination_ad_group_id,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"move_keywords failed: {str(e)}")
        raise


@mcp.tool
def list_budgets(
    customer_id: str,
    include_removed: bool = False,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all campaign budgets in the account."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching budgets for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clause = "" if include_removed else "WHERE campaign_budget.status != 'REMOVED'"

        query = f"""
            SELECT
                campaign_budget.id,
                campaign_budget.name,
                campaign_budget.amount_micros,
                campaign_budget.status,
                campaign_budget.delivery_method,
                campaign_budget.explicitly_shared,
                campaign_budget.reference_count,
                campaign_budget.total_amount_micros
            FROM campaign_budget
            {where_clause}
            ORDER BY campaign_budget.amount_micros DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        budgets = []
        for row in rows:
            b = row.get("campaignBudget", {})
            amount_micros = int(b.get("amountMicros", 0))
            total_micros = int(b.get("totalAmountMicros", 0))

            budgets.append({
                "id": str(b.get("id", "")),
                "name": b.get("name", ""),
                "amount_dollars": round(amount_micros / 1_000_000, 2),
                "status": b.get("status", ""),
                "delivery_method": b.get("deliveryMethod", ""),
                "explicitly_shared": b.get("explicitlyShared", False),
                "reference_count": b.get("referenceCount", 0),
                "total_amount_dollars": round(total_micros / 1_000_000, 2) if total_micros else None,
            })

        if ctx:
            ctx.info(f"Found {len(budgets)} budgets.")

        return {
            "budgets": budgets,
            "total": len(budgets),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_budgets failed: {str(e)}")
        raise


@mcp.tool
def create_shared_budget(
    customer_id: str,
    name: str,
    amount_micros: int,
    delivery_method: str = "STANDARD",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a shared budget that can be applied to multiple campaigns. delivery_method: STANDARD or ACCELERATED."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating shared budget '{name}' ({amount_micros} micros) for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignBudgets:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "name": name,
                        "amountMicros": str(amount_micros),
                        "deliveryMethod": delivery_method.upper(),
                        "explicitlyShared": True,
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
            ctx.info(f"Shared budget created: {resource_name}")

        return {
            "budget_created": resource_name,
            "name": name,
            "amount_dollars": round(amount_micros / 1_000_000, 2),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_shared_budget failed: {str(e)}")
        raise


@mcp.tool
def apply_shared_budget(
    customer_id: str,
    campaign_id: str,
    budget_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Apply a shared budget to a campaign. Use list_budgets to find budget IDs."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Applying budget {budget_id} to campaign {campaign_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaigns:mutate"

        body = {
            "operations": [
                {
                    "update": {
                        "resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                        "campaignBudget": f"customers/{cid}/campaignBudgets/{budget_id}",
                    },
                    "updateMask": "campaign_budget",
                }
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Budget {budget_id} applied to campaign {campaign_id}.")

        return {
            "campaign_id": campaign_id,
            "budget_id": budget_id,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"apply_shared_budget failed: {str(e)}")
        raise
