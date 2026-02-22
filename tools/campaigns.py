"""Campaign & ad group listing/management tools for Google Ads MCP Server."""
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
def list_campaigns(
    customer_id: str,
    status_filter: str = "ENABLED",
    include_removed: bool = False,
    limit: int = 500,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all campaigns with their settings. status_filter: ENABLED, PAUSED, or ALL."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching campaigns for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = []
        if not include_removed:
            where_clauses.append("campaign.status != 'REMOVED'")
        if status_filter.upper() not in ("ALL", ""):
            where_clauses.append(f"campaign.status = '{status_filter.upper()}'")

        where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.advertising_channel_sub_type,
                campaign.bidding_strategy_type,
                campaign.start_date,
                campaign.end_date,
                campaign.campaign_budget,
                campaign.target_roas.target_roas,
                campaign.maximize_conversion_value.target_roas,
                campaign.maximize_conversions.target_cpa_micros,
                campaign.target_cpa.target_cpa_micros,
                campaign.manual_cpc.enhanced_cpc_enabled,
                campaign.serving_status
            FROM campaign
            {where_str}
            ORDER BY campaign.name ASC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        campaigns = []
        for row in rows:
            c = row.get("campaign", {})
            tcpa_micros = (
                c.get("targetCpa", {}).get("targetCpaMicros")
                or c.get("maximizeConversions", {}).get("targetCpaMicros")
            )
            campaigns.append({
                "id": str(c.get("id", "")),
                "name": c.get("name", ""),
                "status": c.get("status", ""),
                "advertising_channel_type": c.get("advertisingChannelType", ""),
                "advertising_channel_sub_type": c.get("advertisingChannelSubType", ""),
                "bidding_strategy_type": c.get("biddingStrategyType", ""),
                "start_date": c.get("startDate", ""),
                "end_date": c.get("endDate", ""),
                "budget_resource": c.get("campaignBudget", ""),
                "target_roas": (
                    c.get("targetRoas", {}).get("targetRoas")
                    or c.get("maximizeConversionValue", {}).get("targetRoas")
                ),
                "target_cpa_dollars": round(int(tcpa_micros) / 1_000_000, 2) if tcpa_micros else None,
                "enhanced_cpc_enabled": c.get("manualCpc", {}).get("enhancedCpcEnabled", False),
                "serving_status": c.get("servingStatus", ""),
            })

        if ctx:
            ctx.info(f"Found {len(campaigns)} campaigns.")

        return {
            "campaigns": campaigns,
            "total": len(campaigns),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_campaigns failed: {str(e)}")
        raise


@mcp.tool
def list_ad_groups(
    customer_id: str,
    campaign_id: str = "",
    status_filter: str = "ENABLED",
    limit: int = 500,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List ad groups with their settings. Optionally filter by campaign."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching ad groups for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")
        if status_filter.upper() not in ("ALL", ""):
            where_clauses.append(f"ad_group.status = '{status_filter.upper()}'")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group.status,
                ad_group.type,
                ad_group.cpc_bid_micros,
                ad_group.cpm_bid_micros,
                ad_group.target_cpa_micros
            FROM ad_group
            WHERE {' AND '.join(where_clauses)}
            ORDER BY campaign.name ASC, ad_group.name ASC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        ad_groups = []
        for row in rows:
            ag = row.get("adGroup", {})
            camp = row.get("campaign", {})
            cpc_micros = int(ag.get("cpcBidMicros", 0))
            cpa_micros = int(ag.get("targetCpaMicros", 0))

            ad_groups.append({
                "id": str(ag.get("id", "")),
                "name": ag.get("name", ""),
                "status": ag.get("status", ""),
                "type": ag.get("type", ""),
                "cpc_bid_dollars": round(cpc_micros / 1_000_000, 4) if cpc_micros else None,
                "target_cpa_dollars": round(cpa_micros / 1_000_000, 2) if cpa_micros else None,
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(ad_groups)} ad groups.")

        return {
            "ad_groups": ad_groups,
            "total": len(ad_groups),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_ad_groups failed: {str(e)}")
        raise


@mcp.tool
def list_keywords(
    customer_id: str,
    campaign_id: str = "",
    ad_group_id: str = "",
    status_filter: str = "ENABLED",
    limit: int = 1000,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List keywords with match types and bids."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching keywords for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
            "ad_group_criterion.status != 'REMOVED'",
            "ad_group_criterion.type = 'KEYWORD'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")
        if ad_group_id:
            where_clauses.append(f"ad_group.id = {ad_group_id}")
        if status_filter.upper() not in ("ALL", ""):
            where_clauses.append(f"ad_group_criterion.status = '{status_filter.upper()}'")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                ad_group_criterion.cpc_bid_micros,
                ad_group_criterion.quality_info.quality_score,
                ad_group_criterion.final_urls
            FROM ad_group_criterion
            WHERE {' AND '.join(where_clauses)}
            ORDER BY ad_group_criterion.keyword.text ASC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        keywords = []
        for row in rows:
            crit = row.get("adGroupCriterion", {})
            kw = crit.get("keyword", {})
            qi = crit.get("qualityInfo", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})
            bid_micros = int(crit.get("cpcBidMicros", 0))

            keywords.append({
                "criterion_id": str(crit.get("criterionId", "")),
                "keyword": kw.get("text", ""),
                "match_type": kw.get("matchType", ""),
                "status": crit.get("status", ""),
                "bid_dollars": round(bid_micros / 1_000_000, 4) if bid_micros else None,
                "quality_score": qi.get("qualityScore"),
                "final_urls": crit.get("finalUrls", []),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(keywords)} keywords.")

        return {
            "keywords": keywords,
            "total": len(keywords),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_keywords failed: {str(e)}")
        raise


@mcp.tool
def list_ads(
    customer_id: str,
    campaign_id: str = "",
    ad_group_id: str = "",
    status_filter: str = "ENABLED",
    limit: int = 500,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all ads with their headlines, descriptions, and status."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching ads for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")
        if ad_group_id:
            where_clauses.append(f"ad_group.id = {ad_group_id}")
        if status_filter.upper() not in ("ALL", ""):
            where_clauses.append(f"ad_group_ad.status = '{status_filter.upper()}'")
        else:
            where_clauses.append("ad_group_ad.status != 'REMOVED'")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.ad.name,
                ad_group_ad.ad.final_urls,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.status,
                ad_group_ad.policy_summary.approval_status
            FROM ad_group_ad
            WHERE {' AND '.join(where_clauses)}
            ORDER BY campaign.name ASC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        ads = []
        for row in rows:
            ada = row.get("adGroupAd", {})
            ad = ada.get("ad", {})
            rsa = ad.get("responsiveSearchAd", {})
            ps = ada.get("policySummary", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            headlines = [h.get("text", "") for h in rsa.get("headlines", [])]
            descriptions = [d.get("text", "") for d in rsa.get("descriptions", [])]

            ads.append({
                "ad_id": str(ad.get("id", "")),
                "ad_type": ad.get("type", ""),
                "ad_name": ad.get("name", ""),
                "final_urls": ad.get("finalUrls", []),
                "headlines": headlines,
                "descriptions": descriptions,
                "status": ada.get("status", ""),
                "approval_status": ps.get("approvalStatus", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
            })

        if ctx:
            ctx.info(f"Found {len(ads)} ads.")

        return {
            "ads": ads,
            "total": len(ads),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_ads failed: {str(e)}")
        raise


@mcp.tool
def update_ad_group(
    customer_id: str,
    ad_group_id: str,
    name: str = "",
    status: str = "",
    cpc_bid_micros: int = -1,
    target_cpa_micros: int = -1,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Update ad group settings. Pass only the fields you want to change. status: ENABLED or PAUSED."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Updating ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroups:mutate"

        update_body: Dict[str, Any] = {
            "resourceName": f"customers/{cid}/adGroups/{ad_group_id}"
        }
        update_mask = []

        if name:
            update_body["name"] = name
            update_mask.append("name")
        if status:
            update_body["status"] = status.upper()
            update_mask.append("status")
        if cpc_bid_micros >= 0:
            update_body["cpcBidMicros"] = str(cpc_bid_micros)
            update_mask.append("cpc_bid_micros")
        if target_cpa_micros >= 0:
            update_body["targetCpaMicros"] = str(target_cpa_micros)
            update_mask.append("target_cpa_micros")

        if not update_mask:
            return {"message": "No fields to update provided.", "customer_id": customer_id}

        body = {
            "operations": [{"update": update_body, "updateMask": ",".join(update_mask)}]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Ad group {ad_group_id} updated: {update_mask}")

        return {
            "ad_group_id": ad_group_id,
            "updated_fields": update_mask,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"update_ad_group failed: {str(e)}")
        raise


@mcp.tool
def set_campaign_end_date(
    customer_id: str,
    campaign_id: str,
    end_date: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Set or update a campaign's end date. end_date format: YYYY-MM-DD. Use '2037-12-30' to effectively remove the end date."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Setting end date for campaign {campaign_id} to {end_date}...")

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
                        "endDate": end_date,
                    },
                    "updateMask": "end_date",
                }
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Campaign {campaign_id} end date set to {end_date}.")

        return {
            "campaign_id": campaign_id,
            "end_date": end_date,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"set_campaign_end_date failed: {str(e)}")
        raise


@mcp.tool
def update_campaign_network_settings(
    customer_id: str,
    campaign_id: str,
    search_network: bool = None,
    search_partners: bool = None,
    display_network: bool = None,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Update campaign network targeting settings. Pass True/False for each network you want to change."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Updating network settings for campaign {campaign_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaigns:mutate"

        network_settings: Dict[str, Any] = {}
        update_mask_fields = []

        if search_network is not None:
            network_settings["targetGoogleSearch"] = search_network
            update_mask_fields.append("network_settings.target_google_search")
        if search_partners is not None:
            network_settings["targetSearchNetwork"] = search_partners
            update_mask_fields.append("network_settings.target_search_network")
        if display_network is not None:
            network_settings["targetContentNetwork"] = display_network
            update_mask_fields.append("network_settings.target_content_network")

        if not update_mask_fields:
            return {"message": "No network settings to update.", "customer_id": customer_id}

        body = {
            "operations": [
                {
                    "update": {
                        "resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                        "networkSettings": network_settings,
                    },
                    "updateMask": ",".join(update_mask_fields),
                }
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Network settings updated for campaign {campaign_id}.")

        return {
            "campaign_id": campaign_id,
            "updated_fields": update_mask_fields,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"update_campaign_network_settings failed: {str(e)}")
        raise
