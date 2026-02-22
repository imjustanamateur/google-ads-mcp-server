"""Ad asset/extension management tools for Google Ads MCP Server."""
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
def list_assets(
    customer_id: str,
    asset_type: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all assets in the account. asset_type filter: TEXT, IMAGE, YOUTUBE_VIDEO, MEDIA_BUNDLE, CALLOUT, STRUCTURED_SNIPPET, SITELINK, CALL, APP, PRICE, PROMOTION, IMAGE."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching assets for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = ["asset.status != 'REMOVED'"]
        if asset_type:
            where_clauses.append(f"asset.type = '{asset_type.upper()}'")

        query = f"""
            SELECT
                asset.id,
                asset.name,
                asset.type,
                asset.status,
                asset.text_asset.text,
                asset.image_asset.full_size.url,
                asset.youtube_video_asset.youtube_video_id,
                asset.youtube_video_asset.youtube_video_title
            FROM asset
            WHERE {' AND '.join(where_clauses)}
            ORDER BY asset.name ASC
            LIMIT 500
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        assets = []
        for row in rows:
            a = row.get("asset", {})
            text = a.get("textAsset", {})
            image = a.get("imageAsset", {})
            full_size = image.get("fullSize", {})
            video = a.get("youtubeVideoAsset", {})

            assets.append({
                "id": str(a.get("id", "")),
                "name": a.get("name", ""),
                "type": a.get("type", ""),
                "status": a.get("status", ""),
                "text": text.get("text", ""),
                "image_url": full_size.get("url", ""),
                "youtube_video_id": video.get("youtubeVideoId", ""),
                "youtube_video_title": video.get("youtubeVideoTitle", ""),
            })

        if ctx:
            ctx.info(f"Found {len(assets)} assets.")

        return {
            "assets": assets,
            "total": len(assets),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_assets failed: {str(e)}")
        raise


@mcp.tool
def create_image_asset(
    customer_id: str,
    name: str,
    image_url: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create an image asset from a URL. The image will be downloaded and uploaded to Google Ads."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating image asset '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        # Download the image
        img_resp = requests.get(image_url, timeout=30)
        if not img_resp.ok:
            raise Exception(f"Failed to download image from {image_url}: {img_resp.status_code}")

        import base64
        image_data = base64.standard_b64encode(img_resp.content).decode("utf-8")

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/assets:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "name": name,
                        "type": "IMAGE",
                        "imageAsset": {
                            "data": image_data,
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
            ctx.info(f"Image asset created: {resource_name}")

        return {
            "asset_created": resource_name,
            "name": name,
            "image_url": image_url,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_image_asset failed: {str(e)}")
        raise


@mcp.tool
def add_price_extension(
    customer_id: str,
    campaign_id: str,
    price_qualifier: str,
    items: List[Dict[str, Any]],
    language_code: str = "EN",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Add a price extension to a campaign. price_qualifier: FROM, UP_TO, AVERAGE. items: list of {header, description, price_micros, currency_code, final_url}."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Adding price extension to campaign {campaign_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        price_items = []
        for item in items:
            price_items.append({
                "header": item.get("header", ""),
                "description": item.get("description", ""),
                "price": {
                    "amountMicros": int(item.get("price_micros", 0)),
                    "currencyCode": item.get("currency_code", "USD"),
                },
                "unit": item.get("unit", "UNKNOWN"),
                "finalUrls": [item.get("final_url", "")],
            })

        # Create price asset
        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/assets:mutate"
        asset_body = {
            "operations": [
                {
                    "create": {
                        "type": "PRICE",
                        "priceAsset": {
                            "type": "BRANDS",
                            "priceQualifier": price_qualifier.upper(),
                            "languageCode": language_code,
                            "priceOfferings": price_items,
                        },
                    }
                }
            ]
        }

        resp = _make_request(requests.post, asset_url, headers, asset_body)
        if not resp.ok:
            raise Exception(f"API error creating price asset: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [{}])
        asset_resource = results[0].get("resourceName", "") if results else ""

        # Link asset to campaign
        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignAssets:mutate"
        link_body = {
            "operations": [
                {
                    "create": {
                        "campaign": f"customers/{cid}/campaigns/{campaign_id}",
                        "asset": asset_resource,
                        "fieldType": "PRICE",
                    }
                }
            ]
        }

        link_resp = _make_request(requests.post, link_url, headers, link_body)
        if not link_resp.ok:
            raise Exception(f"API error linking price asset: {link_resp.status_code} {link_resp.text}")

        if ctx:
            ctx.info(f"Price extension added to campaign {campaign_id}.")

        return {
            "asset_resource": asset_resource,
            "campaign_id": campaign_id,
            "items_count": len(items),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"add_price_extension failed: {str(e)}")
        raise


@mcp.tool
def add_promotion_extension(
    customer_id: str,
    campaign_id: str,
    promotion_target: str,
    discount_modifier: str = "UNMODIFIED",
    percent_off: int = 0,
    money_amount_off_micros: int = 0,
    final_url: str = "",
    start_date: str = "",
    end_date: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Add a promotion extension to a campaign. Use either percent_off (e.g. 20 for 20% off) or money_amount_off_micros."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if not percent_off and not money_amount_off_micros:
        raise ValueError("Must provide either percent_off or money_amount_off_micros.")

    if ctx:
        ctx.info(f"Adding promotion extension to campaign {campaign_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        promotion_asset: Dict[str, Any] = {
            "promotionTarget": promotion_target,
            "discountModifier": discount_modifier,
        }
        if percent_off:
            promotion_asset["percentOff"] = percent_off * 1_000_000  # micros
        elif money_amount_off_micros:
            promotion_asset["moneyAmountOff"] = {
                "amountMicros": money_amount_off_micros,
                "currencyCode": "USD",
            }
        if final_url:
            promotion_asset["finalUrls"] = [final_url]
        if start_date:
            promotion_asset["promotionStartDate"] = start_date
        if end_date:
            promotion_asset["promotionEndDate"] = end_date

        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/assets:mutate"
        asset_body = {
            "operations": [
                {
                    "create": {
                        "type": "PROMOTION",
                        "promotionAsset": promotion_asset,
                    }
                }
            ]
        }

        resp = _make_request(requests.post, asset_url, headers, asset_body)
        if not resp.ok:
            raise Exception(f"API error creating promotion asset: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [{}])
        asset_resource = results[0].get("resourceName", "") if results else ""

        # Link to campaign
        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignAssets:mutate"
        link_body = {
            "operations": [
                {
                    "create": {
                        "campaign": f"customers/{cid}/campaigns/{campaign_id}",
                        "asset": asset_resource,
                        "fieldType": "PROMOTION",
                    }
                }
            ]
        }

        link_resp = _make_request(requests.post, link_url, headers, link_body)
        if not link_resp.ok:
            raise Exception(f"API error linking promotion asset: {link_resp.status_code} {link_resp.text}")

        if ctx:
            ctx.info(f"Promotion extension added to campaign {campaign_id}.")

        return {
            "asset_resource": asset_resource,
            "campaign_id": campaign_id,
            "promotion_target": promotion_target,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"add_promotion_extension failed: {str(e)}")
        raise


@mcp.tool
def add_image_extension(
    customer_id: str,
    campaign_id: str,
    asset_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Link an existing image asset to a campaign as an image extension. Use list_assets to find asset IDs or create_image_asset to create a new one."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Linking image asset {asset_id} to campaign {campaign_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/campaignAssets:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "campaign": f"customers/{cid}/campaigns/{campaign_id}",
                        "asset": f"customers/{cid}/assets/{asset_id}",
                        "fieldType": "IMAGE",
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
            ctx.info(f"Image extension linked: {resource_name}")

        return {
            "campaign_asset_resource": resource_name,
            "campaign_id": campaign_id,
            "asset_id": asset_id,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"add_image_extension failed: {str(e)}")
        raise
