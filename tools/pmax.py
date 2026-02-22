"""Performance Max campaign tools for Google Ads MCP Server."""
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
def create_pmax_campaign(
    customer_id: str,
    name: str,
    budget_micros: int,
    target_roas: float = 0.0,
    target_cpa_micros: int = 0,
    final_url_expansion_opt_out: bool = False,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a Performance Max campaign. Use either target_roas (e.g. 3.5 for 350% ROAS) or target_cpa_micros."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating Performance Max campaign '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        # Step 1: Create budget
        budget_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignBudgets:mutate"
        budget_body = {
            "operations": [
                {
                    "create": {
                        "name": f"{name} Budget",
                        "amountMicros": str(budget_micros),
                        "deliveryMethod": "STANDARD",
                        "explicitlyShared": False,
                    }
                }
            ]
        }

        budget_resp = _make_request(requests.post, budget_url, headers, budget_body)
        if not budget_resp.ok:
            raise Exception(f"Budget creation error: {budget_resp.status_code} {budget_resp.text}")

        budget_resource = budget_resp.json()["results"][0]["resourceName"]

        # Step 2: Create campaign
        campaign_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaigns:mutate"

        campaign_body_data: Dict[str, Any] = {
            "name": name,
            "status": "PAUSED",
            "advertisingChannelType": "PERFORMANCE_MAX",
            "campaignBudget": budget_resource,
            "finalUrlExpansionOptOut": final_url_expansion_opt_out,
        }

        if target_roas > 0:
            campaign_body_data["maximizeConversionValue"] = {"targetRoas": target_roas}
        elif target_cpa_micros > 0:
            campaign_body_data["maximizeConversions"] = {"targetCpaMicros": str(target_cpa_micros)}
        else:
            campaign_body_data["maximizeConversionValue"] = {}

        campaign_body = {"operations": [{"create": campaign_body_data}]}
        campaign_resp = _make_request(requests.post, campaign_url, headers, campaign_body)
        if not campaign_resp.ok:
            raise Exception(f"Campaign creation error: {campaign_resp.status_code} {campaign_resp.text}")

        campaign_resource = campaign_resp.json()["results"][0]["resourceName"]
        campaign_id = campaign_resource.split("/")[-1]

        if ctx:
            ctx.info(f"PMax campaign created: {campaign_resource}")

        return {
            "campaign_created": campaign_resource,
            "campaign_id": campaign_id,
            "budget_created": budget_resource,
            "name": name,
            "status": "PAUSED",
            "note": "Campaign is PAUSED. Create asset groups before enabling.",
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_pmax_campaign failed: {str(e)}")
        raise


@mcp.tool
def create_pmax_asset_group(
    customer_id: str,
    campaign_id: str,
    name: str,
    final_urls: List[str],
    headlines: List[str],
    descriptions: List[str],
    business_name: str,
    marketing_image_asset_ids: List[str],
    logo_image_asset_ids: List[str] = None,
    long_headlines: List[str] = None,
    path1: str = "",
    path2: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a PMax asset group. headlines: 3-5 items, descriptions: 2-5 items, final_urls: at least 1. Use list_assets or create_image_asset to get image asset IDs."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if len(headlines) < 3:
        raise ValueError("At least 3 headlines are required.")
    if len(descriptions) < 2:
        raise ValueError("At least 2 descriptions are required.")
    if not final_urls:
        raise ValueError("At least one final URL is required.")
    if not marketing_image_asset_ids:
        raise ValueError("At least one marketing image asset ID is required.")

    if ctx:
        ctx.info(f"Creating PMax asset group '{name}' in campaign {campaign_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/assetGroups:mutate"

        asset_group: Dict[str, Any] = {
            "name": name,
            "campaign": f"customers/{cid}/campaigns/{campaign_id}",
            "status": "ENABLED",
            "finalUrls": final_urls,
            "headlines": [{"text": h} for h in headlines[:15]],
            "descriptions": [{"text": d} for d in descriptions[:5]],
            "businessName": business_name,
            "marketingImages": [
                {"asset": f"customers/{cid}/assets/{aid}"}
                for aid in marketing_image_asset_ids
            ],
        }

        if long_headlines:
            asset_group["longHeadlines"] = [{"text": h} for h in long_headlines[:5]]
        if logo_image_asset_ids:
            asset_group["logoImages"] = [
                {"asset": f"customers/{cid}/assets/{aid}"}
                for aid in logo_image_asset_ids
            ]
        if path1:
            asset_group["path1"] = path1
        if path2:
            asset_group["path2"] = path2

        body = {"operations": [{"create": asset_group}]}

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [{}])
        resource_name = results[0].get("resourceName", "") if results else ""
        asset_group_id = resource_name.split("/")[-1] if resource_name else ""

        if ctx:
            ctx.info(f"PMax asset group created: {resource_name}")

        return {
            "asset_group_created": resource_name,
            "asset_group_id": asset_group_id,
            "campaign_id": campaign_id,
            "name": name,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_pmax_asset_group failed: {str(e)}")
        raise


@mcp.tool
def list_pmax_asset_groups(
    customer_id: str,
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all Performance Max asset groups with their status."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching PMax asset groups for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
            "campaign.status != 'REMOVED'",
            "asset_group.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")

        query = f"""
            SELECT
                asset_group.id,
                asset_group.name,
                asset_group.status,
                asset_group.final_urls,
                asset_group.path1,
                asset_group.path2,
                campaign.id,
                campaign.name
            FROM asset_group
            WHERE {' AND '.join(where_clauses)}
            ORDER BY campaign.name ASC, asset_group.name ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        asset_groups = []
        for row in rows:
            ag = row.get("assetGroup", {})
            camp = row.get("campaign", {})

            asset_groups.append({
                "id": str(ag.get("id", "")),
                "name": ag.get("name", ""),
                "status": ag.get("status", ""),
                "final_urls": ag.get("finalUrls", []),
                "path1": ag.get("path1", ""),
                "path2": ag.get("path2", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(asset_groups)} PMax asset groups.")

        return {
            "asset_groups": asset_groups,
            "total": len(asset_groups),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_pmax_asset_groups failed: {str(e)}")
        raise
