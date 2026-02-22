"""Label management tools for Google Ads MCP Server."""
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
def list_labels(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all labels in the account."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching labels for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = """
            SELECT
                label.id,
                label.name,
                label.status,
                label.text_label.background_color,
                label.text_label.description
            FROM label
            WHERE label.status != 'REMOVED'
            ORDER BY label.name ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        labels = []
        for row in rows:
            lbl = row.get("label", {})
            txt = lbl.get("textLabel", {})
            labels.append({
                "id": str(lbl.get("id", "")),
                "name": lbl.get("name", ""),
                "status": lbl.get("status", ""),
                "background_color": txt.get("backgroundColor", ""),
                "description": txt.get("description", ""),
            })

        if ctx:
            ctx.info(f"Found {len(labels)} labels.")

        return {
            "labels": labels,
            "total": len(labels),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_labels failed: {str(e)}")
        raise


@mcp.tool
def create_label(
    customer_id: str,
    name: str,
    description: str = "",
    background_color: str = "#ffffff",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a new label. background_color should be a hex color like '#4285F4'."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Creating label '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/labels:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "name": name,
                        "textLabel": {
                            "backgroundColor": background_color,
                            "description": description,
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
            ctx.info(f"Label created: {resource_name}")

        return {
            "label_created": resource_name,
            "name": name,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_label failed: {str(e)}")
        raise


@mcp.tool
def apply_label(
    customer_id: str,
    label_id: str,
    resource_type: str,
    resource_ids: List[str],
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Apply a label to campaigns, ad groups, ads, or keywords. resource_type: 'campaign', 'ad_group', 'ad', 'keyword'."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    valid_types = {"campaign", "ad_group", "ad", "keyword"}
    if resource_type not in valid_types:
        raise ValueError(f"resource_type must be one of: {', '.join(sorted(valid_types))}")

    if ctx:
        ctx.info(f"Applying label {label_id} to {len(resource_ids)} {resource_type}(s) for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        label_resource = f"customers/{cid}/labels/{label_id}"

        endpoint_map = {
            "campaign": ("campaignLabels", "campaign"),
            "ad_group": ("adGroupLabels", "adGroup"),
            "ad": ("adGroupAdLabels", "adGroupAd"),
            "keyword": ("adGroupCriterionLabels", "adGroupCriterion"),
        }
        endpoint_suffix, resource_field = endpoint_map[resource_type]

        resource_prefix_map = {
            "campaign": f"customers/{cid}/campaigns",
            "ad_group": f"customers/{cid}/adGroups",
            "ad": f"customers/{cid}/adGroupAds",
            "keyword": f"customers/{cid}/adGroupCriteria",
        }
        prefix = resource_prefix_map[resource_type]

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/{endpoint_suffix}:mutate"

        operations = [
            {
                "create": {
                    resource_field: f"{prefix}/{rid}",
                    "label": label_resource,
                }
            }
            for rid in resource_ids
        ]

        body = {"operations": operations}

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [])

        if ctx:
            ctx.info(f"Applied label to {len(results)} resource(s).")

        return {
            "label_applied_count": len(results),
            "label_id": label_id,
            "resource_type": resource_type,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"apply_label failed: {str(e)}")
        raise


@mcp.tool
def remove_label(
    customer_id: str,
    label_id: str,
    resource_type: str,
    resource_ids: List[str],
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Remove a label from campaigns, ad groups, ads, or keywords. resource_type: 'campaign', 'ad_group', 'ad', 'keyword'."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    valid_types = {"campaign", "ad_group", "ad", "keyword"}
    if resource_type not in valid_types:
        raise ValueError(f"resource_type must be one of: {', '.join(sorted(valid_types))}")

    if ctx:
        ctx.info(f"Removing label {label_id} from {len(resource_ids)} {resource_type}(s) for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        endpoint_map = {
            "campaign": "campaignLabels",
            "ad_group": "adGroupLabels",
            "ad": "adGroupAdLabels",
            "keyword": "adGroupCriterionLabels",
        }
        endpoint_suffix = endpoint_map[resource_type]

        resource_prefix_map = {
            "campaign": f"customers/{cid}/campaignLabels",
            "ad_group": f"customers/{cid}/adGroupLabels",
            "ad": f"customers/{cid}/adGroupAdLabels",
            "keyword": f"customers/{cid}/adGroupCriterionLabels",
        }
        prefix = resource_prefix_map[resource_type]

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/{endpoint_suffix}:mutate"

        operations = [
            {"remove": f"{prefix}/{rid}~{label_id}"}
            for rid in resource_ids
        ]

        body = {"operations": operations}

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [])

        if ctx:
            ctx.info(f"Removed label from {len(results)} resource(s).")

        return {
            "label_removed_count": len(results),
            "label_id": label_id,
            "resource_type": resource_type,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"remove_label failed: {str(e)}")
        raise
