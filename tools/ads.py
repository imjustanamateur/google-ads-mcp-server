"""Ad creation and management tools for Google Ads MCP Server."""
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
def update_responsive_search_ad(
    customer_id: str,
    ad_id: str,
    ad_group_id: str,
    headlines: List[str] = None,
    descriptions: List[str] = None,
    final_urls: List[str] = None,
    path1: str = "",
    path2: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Update an existing Responsive Search Ad. Pass only the fields you want to change. Headlines: 3-15 items. Descriptions: 2-4 items."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Updating RSA {ad_id} in ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupAds:mutate"

        ad_update: Dict[str, Any] = {
            "resourceName": f"customers/{cid}/adGroupAds/{ad_group_id}~{ad_id}",
            "ad": {"id": str(ad_id), "responsiveSearchAd": {}},
        }
        update_mask = []

        if headlines:
            ad_update["ad"]["responsiveSearchAd"]["headlines"] = [
                {"text": h} for h in headlines
            ]
            update_mask.append("ad.responsive_search_ad.headlines")
        if descriptions:
            ad_update["ad"]["responsiveSearchAd"]["descriptions"] = [
                {"text": d} for d in descriptions
            ]
            update_mask.append("ad.responsive_search_ad.descriptions")
        if final_urls:
            ad_update["ad"]["finalUrls"] = final_urls
            update_mask.append("ad.final_urls")
        if path1:
            ad_update["ad"]["responsiveSearchAd"]["path1"] = path1
            update_mask.append("ad.responsive_search_ad.path1")
        if path2:
            ad_update["ad"]["responsiveSearchAd"]["path2"] = path2
            update_mask.append("ad.responsive_search_ad.path2")

        if not update_mask:
            return {"message": "No fields to update provided.", "customer_id": customer_id}

        body = {
            "operations": [
                {"update": ad_update, "updateMask": ",".join(update_mask)}
            ]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"RSA {ad_id} updated: {update_mask}")

        return {
            "ad_id": ad_id,
            "ad_group_id": ad_group_id,
            "updated_fields": update_mask,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"update_responsive_search_ad failed: {str(e)}")
        raise


@mcp.tool
def get_ad_strength(
    customer_id: str,
    campaign_id: str = "",
    ad_group_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get Ad Strength scores for Responsive Search Ads."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching ad strength for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            "campaign.status != 'REMOVED'",
            "ad_group_ad.status != 'REMOVED'",
            "ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")
        if ad_group_id:
            where_clauses.append(f"ad_group.id = {ad_group_id}")

        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad_strength,
                ad_group_ad.status,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name
            FROM ad_group_ad
            WHERE {' AND '.join(where_clauses)}
            ORDER BY ad_group_ad.ad_strength ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        ads = []
        for row in rows:
            ada = row.get("adGroupAd", {})
            ad = ada.get("ad", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            ads.append({
                "ad_id": str(ad.get("id", "")),
                "ad_name": ad.get("name", ""),
                "ad_strength": ada.get("adStrength", ""),
                "status": ada.get("status", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
            })

        if ctx:
            ctx.info(f"Retrieved ad strength for {len(ads)} RSAs.")

        return {
            "ads": ads,
            "total": len(ads),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_ad_strength failed: {str(e)}")
        raise


@mcp.tool
def create_responsive_display_ad(
    customer_id: str,
    ad_group_id: str,
    headlines: List[str],
    descriptions: List[str],
    business_name: str,
    final_url: str,
    marketing_image_asset_id: str,
    logo_image_asset_id: str = "",
    long_headline: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a Responsive Display Ad for Display/Gmail campaigns. headlines: 1-5 items, descriptions: 1-5 items. Use list_assets to find image asset IDs or create_image_asset to upload new images."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if not headlines or not descriptions:
        raise ValueError("headlines and descriptions are required.")

    if ctx:
        ctx.info(f"Creating Responsive Display Ad in ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupAds:mutate"

        rda: Dict[str, Any] = {
            "headlines": [{"text": h} for h in headlines[:5]],
            "descriptions": [{"text": d} for d in descriptions[:5]],
            "businessName": business_name,
            "marketingImages": [
                {"asset": f"customers/{cid}/assets/{marketing_image_asset_id}"}
            ],
        }
        if long_headline:
            rda["longHeadline"] = {"text": long_headline}
        if logo_image_asset_id:
            rda["logoImages"] = [
                {"asset": f"customers/{cid}/assets/{logo_image_asset_id}"}
            ]

        body = {
            "operations": [
                {
                    "create": {
                        "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                        "status": "ENABLED",
                        "ad": {
                            "finalUrls": [final_url],
                            "responsiveDisplayAd": rda,
                        },
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
            ctx.info(f"Responsive Display Ad created: {resource_name}")

        return {
            "ad_created": resource_name,
            "ad_group_id": ad_group_id,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_responsive_display_ad failed: {str(e)}")
        raise


@mcp.tool
def create_call_only_ad(
    customer_id: str,
    ad_group_id: str,
    phone_number: str,
    country_code: str,
    business_name: str,
    headline1: str,
    headline2: str,
    description1: str,
    description2: str = "",
    final_url: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a Call-Only Ad for call-focused campaigns. phone_number: e.g. '555-555-5555'. country_code: e.g. 'US'."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating Call-Only Ad in ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupAds:mutate"

        call_ad: Dict[str, Any] = {
            "phoneNumber": phone_number,
            "countryCode": country_code,
            "businessName": business_name,
            "headline1": headline1,
            "headline2": headline2,
            "description1": description1,
        }
        if description2:
            call_ad["description2"] = description2
        if final_url:
            call_ad["finalUrls"] = [final_url]

        body = {
            "operations": [
                {
                    "create": {
                        "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                        "status": "ENABLED",
                        "ad": {
                            "callAd": call_ad,
                        },
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
            ctx.info(f"Call-Only Ad created: {resource_name}")

        return {
            "ad_created": resource_name,
            "ad_group_id": ad_group_id,
            "phone_number": phone_number,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_call_only_ad failed: {str(e)}")
        raise


@mcp.tool
def apply_recommendation(
    customer_id: str,
    recommendation_resource_name: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Apply a Google Ads recommendation. Use get_recommendations to get resource names."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Applying recommendation {recommendation_resource_name} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/recommendations:apply"

        body = {
            "operations": [{"resourceName": recommendation_resource_name}]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Recommendation applied successfully.")

        return {
            "recommendation_applied": recommendation_resource_name,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"apply_recommendation failed: {str(e)}")
        raise


@mcp.tool
def dismiss_recommendation(
    customer_id: str,
    recommendation_resource_name: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Dismiss a Google Ads recommendation. Use get_recommendations to get resource names."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Dismissing recommendation {recommendation_resource_name} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/recommendations:dismiss"

        body = {
            "operations": [{"resourceName": recommendation_resource_name}]
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Recommendation dismissed.")

        return {
            "recommendation_dismissed": recommendation_resource_name,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"dismiss_recommendation failed: {str(e)}")
        raise
