"""Shopping campaign tools for Google Ads MCP Server."""
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
def create_shopping_campaign(
    customer_id: str,
    name: str,
    budget_micros: int,
    merchant_id: str,
    sales_country: str = "US",
    campaign_priority: int = 0,
    enable_local_inventory_ads: bool = False,
    target_roas: float = 0.0,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a Standard Shopping campaign. campaign_priority: 0 (low), 1 (medium), 2 (high). Use target_roas for Target ROAS bidding (e.g. 3.5 for 350%)."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating Shopping campaign '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        # Step 1: Create budget
        budget_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignBudgets:mutate"
        budget_resp = _make_request(requests.post, budget_url, headers, {
            "operations": [{
                "create": {
                    "name": f"{name} Budget",
                    "amountMicros": str(budget_micros),
                    "deliveryMethod": "STANDARD",
                    "explicitlyShared": False,
                }
            }]
        })
        if not budget_resp.ok:
            raise Exception(f"Budget creation error: {budget_resp.status_code} {budget_resp.text}")

        budget_resource = budget_resp.json()["results"][0]["resourceName"]

        # Step 2: Create campaign
        campaign_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaigns:mutate"

        campaign_data: Dict[str, Any] = {
            "name": name,
            "status": "PAUSED",
            "advertisingChannelType": "SHOPPING",
            "campaignBudget": budget_resource,
            "shoppingSetting": {
                "merchantId": int(merchant_id),
                "salesCountry": sales_country,
                "campaignPriority": campaign_priority,
                "enableLocal": enable_local_inventory_ads,
            },
        }

        if target_roas > 0:
            campaign_data["targetRoas"] = {"targetRoas": target_roas}
        else:
            campaign_data["manualCpc"] = {"enhancedCpcEnabled": True}

        campaign_resp = _make_request(requests.post, campaign_url, headers, {
            "operations": [{"create": campaign_data}]
        })
        if not campaign_resp.ok:
            raise Exception(f"Campaign creation error: {campaign_resp.status_code} {campaign_resp.text}")

        campaign_resource = campaign_resp.json()["results"][0]["resourceName"]
        campaign_id = campaign_resource.split("/")[-1]

        if ctx:
            ctx.info(f"Shopping campaign created: {campaign_resource}")

        return {
            "campaign_created": campaign_resource,
            "campaign_id": campaign_id,
            "budget_created": budget_resource,
            "name": name,
            "status": "PAUSED",
            "merchant_id": merchant_id,
            "sales_country": sales_country,
            "note": "Campaign is PAUSED. Add ad groups and product groups before enabling.",
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_shopping_campaign failed: {str(e)}")
        raise


@mcp.tool
def list_product_groups(
    customer_id: str,
    campaign_id: str = "",
    ad_group_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List product groups (listing groups) in Shopping campaigns with their bids and performance."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching product groups for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
            "ad_group_criterion.status != 'REMOVED'",
            "ad_group_criterion.type = 'LISTING_GROUP'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")
        if ad_group_id:
            where_clauses.append(f"ad_group.id = {ad_group_id}")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.criterion_id,
                ad_group_criterion.listing_group.type,
                ad_group_criterion.listing_group.case_value.product_brand.value,
                ad_group_criterion.listing_group.case_value.product_type.value,
                ad_group_criterion.listing_group.case_value.product_item_id.value,
                ad_group_criterion.cpc_bid_micros,
                ad_group_criterion.status
            FROM ad_group_criterion
            WHERE {' AND '.join(where_clauses)}
            ORDER BY campaign.name ASC
            LIMIT 1000
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        product_groups = []
        for row in rows:
            crit = row.get("adGroupCriterion", {})
            lg = crit.get("listingGroup", {})
            cv = lg.get("caseValue", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})
            bid_micros = int(crit.get("cpcBidMicros", 0))

            # Extract partition value
            partition_value = ""
            if "productBrand" in cv:
                partition_value = cv["productBrand"].get("value", "")
            elif "productType" in cv:
                partition_value = cv["productType"].get("value", "")
            elif "productItemId" in cv:
                partition_value = cv["productItemId"].get("value", "")

            product_groups.append({
                "criterion_id": str(crit.get("criterionId", "")),
                "listing_group_type": lg.get("type", ""),
                "partition_value": partition_value,
                "bid_dollars": round(bid_micros / 1_000_000, 4) if bid_micros else None,
                "status": crit.get("status", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(product_groups)} product groups.")

        return {
            "product_groups": product_groups,
            "total": len(product_groups),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_product_groups failed: {str(e)}")
        raise
