"""Audience & remarketing tools for Google Ads MCP Server."""
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
def list_user_lists(
    customer_id: str,
    include_closed: bool = False,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """List remarketing and customer match user lists."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching user lists for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clause = "" if include_closed else "WHERE user_list.membership_status = 'OPEN'"

        query = f"""
            SELECT
                user_list.id,
                user_list.name,
                user_list.description,
                user_list.type,
                user_list.membership_status,
                user_list.size_for_search,
                user_list.size_for_display,
                user_list.eligible_for_search,
                user_list.eligible_for_display
            FROM user_list
            {where_clause}
            ORDER BY user_list.size_for_search DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        user_lists = []
        for row in rows:
            ul = row.get("userList", {})
            user_lists.append({
                "id": str(ul.get("id", "")),
                "name": ul.get("name", ""),
                "description": ul.get("description", ""),
                "type": ul.get("type", ""),
                "membership_status": ul.get("membershipStatus", ""),
                "size_for_search": ul.get("sizeForSearch"),
                "size_for_display": ul.get("sizeForDisplay"),
                "eligible_for_search": ul.get("eligibleForSearch", False),
                "eligible_for_display": ul.get("eligibleForDisplay", False),
            })

        if ctx:
            ctx.info(f"Found {len(user_lists)} user lists.")

        return {
            "user_lists": user_lists,
            "total": len(user_lists),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_user_lists failed: {str(e)}")
        raise


@mcp.tool
def create_customer_match_list(
    customer_id: str,
    name: str,
    description: str = "",
    membership_life_span_days: int = 30,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Create a new Customer Match user list for uploading first-party data. membership_life_span_days: how long members stay in the list (1-540 days)."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    membership_life_span_days = max(1, min(540, membership_life_span_days))

    if ctx:
        ctx.info(f"Creating Customer Match user list '{name}' for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/userLists:mutate"

        body = {
            "operations": [
                {
                    "create": {
                        "name": name,
                        "description": description,
                        "membershipLifeSpan": membership_life_span_days,
                        "crmBasedUserList": {
                            "uploadKeyType": "CONTACT_INFO",
                            "dataSourceType": "FIRST_PARTY",
                        },
                        "membershipStatus": "OPEN",
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
            ctx.info(f"Customer Match list created: {resource_name}")

        return {
            "user_list_created": resource_name,
            "name": name,
            "description": description,
            "membership_life_span_days": membership_life_span_days,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"create_customer_match_list failed: {str(e)}")
        raise


@mcp.tool
def add_topic_targeting(
    customer_id: str,
    ad_group_id: str,
    topic_ids: List[int],
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Add topic targeting to an ad group (Display/YouTube campaigns). Find topic IDs via run_gaql: SELECT topic_constant.id, topic_constant.path FROM topic_constant."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Adding {len(topic_ids)} topic(s) to ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupCriteria:mutate"

        operations = [
            {
                "create": {
                    "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                    "topic": {
                        "topicConstant": f"topicConstants/{topic_id}"
                    },
                }
            }
            for topic_id in topic_ids
        ]

        body = {"operations": operations}

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [])

        if ctx:
            ctx.info(f"Added {len(results)} topic(s).")

        return {
            "topics_added": len(results),
            "ad_group_id": ad_group_id,
            "topic_ids": topic_ids,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"add_topic_targeting failed: {str(e)}")
        raise


@mcp.tool
def add_placement_targeting(
    customer_id: str,
    ad_group_id: str,
    placements: List[str],
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Add placement targeting to an ad group (Display/YouTube). placements: list of URLs or YouTube channel/video URLs. Example: ['www.example.com', 'youtube.com/channel/UCxxxxxx']."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Adding {len(placements)} placement(s) to ad group {ad_group_id} for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/adGroupCriteria:mutate"

        operations = [
            {
                "create": {
                    "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                    "placement": {"url": placement},
                }
            }
            for placement in placements
        ]

        body = {"operations": operations}

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        results = resp.json().get("results", [])

        if ctx:
            ctx.info(f"Added {len(results)} placement(s).")

        return {
            "placements_added": len(results),
            "ad_group_id": ad_group_id,
            "placements": placements,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"add_placement_targeting failed: {str(e)}")
        raise


@mcp.tool
def list_audience_segments(
    customer_id: str,
    segment_type: str = "IN_MARKET",
    name_filter: str = "",
    limit: int = 50,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Browse available audience segments. segment_type: IN_MARKET (in-market audiences) or AFFINITY. Use returned IDs with add_audience_targeting."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    segment_type = segment_type.upper()
    valid_types = {"IN_MARKET", "AFFINITY"}
    if segment_type not in valid_types:
        raise ValueError(f"segment_type must be one of: {', '.join(sorted(valid_types))}")

    if ctx:
        ctx.info(f"Fetching {segment_type} audience segments for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        name_clause = f"AND user_interest.name LIKE '%{name_filter}%'" if name_filter else ""

        query = f"""
            SELECT
                user_interest.user_interest_id,
                user_interest.name,
                user_interest.taxonomy_type
            FROM user_interest
            WHERE user_interest.taxonomy_type = '{segment_type}'
            {name_clause}
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        segments = []
        for row in rows:
            ui = row.get("userInterest", {})
            segments.append({
                "user_interest_id": str(ui.get("userInterestId", "")),
                "name": ui.get("name", ""),
                "taxonomy_type": ui.get("taxonomyType", ""),
            })

        if ctx:
            ctx.info(f"Found {len(segments)} {segment_type} segments.")

        return {
            "segments": segments,
            "total": len(segments),
            "segment_type": segment_type,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_audience_segments failed: {str(e)}")
        raise
