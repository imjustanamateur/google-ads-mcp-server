"""Reporting & Analytics tools for Google Ads MCP Server."""
import logging
from typing import Any, Dict, Optional
from fastmcp import Context
from mcp_instance import mcp
from oauth.google_auth import (
    format_customer_id,
    execute_gaql,
    GOOGLE_ADS_DEVELOPER_TOKEN,
)

logger = logging.getLogger(__name__)

VALID_DATE_RANGES = {
    "TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS",
    "LAST_WEEK_SUN_SAT", "LAST_WEEK_MON_SUN", "THIS_WEEK_SUN_TODAY",
    "THIS_WEEK_MON_TODAY", "THIS_MONTH", "LAST_MONTH",
}


@mcp.tool
def get_keyword_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    ad_group_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get keyword-level performance metrics including impressions, clicks, cost, conversions."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching keyword performance for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            f"segments.date DURING {date_range}",
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
            "ad_group_criterion.status != 'REMOVED'",
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
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.cpc_bid_micros,
                ad_group_criterion.quality_info.quality_score,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc,
                metrics.average_position
            FROM keyword_view
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        keywords = []
        for row in rows:
            m = row.get("metrics", {})
            crit = row.get("adGroupCriterion", {})
            kw = crit.get("keyword", {})
            qi = crit.get("qualityInfo", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            cost_micros = int(m.get("costMicros", 0))
            bid_micros = int(crit.get("cpcBidMicros", 0))
            avg_cpc_micros = int(m.get("averageCpc", 0))

            keywords.append({
                "keyword": kw.get("text", ""),
                "match_type": kw.get("matchType", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
                "bid_dollars": round(bid_micros / 1_000_000, 4),
                "quality_score": qi.get("qualityScore"),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
                "avg_cpc_dollars": round(avg_cpc_micros / 1_000_000, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(keywords)} keywords.")

        return {
            "keywords": keywords,
            "total": len(keywords),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_keyword_performance failed: {str(e)}")
        raise


@mcp.tool
def get_ad_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    ad_group_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get ad-level performance metrics for all active ads."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching ad performance for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            f"segments.date DURING {date_range}",
            "campaign.status != 'REMOVED'",
            "ad_group_ad.status != 'REMOVED'",
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
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.ad.name,
                ad_group_ad.ad.final_urls,
                ad_group_ad.status,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc
            FROM ad_group_ad
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        ads = []
        for row in rows:
            m = row.get("metrics", {})
            ada = row.get("adGroupAd", {})
            ad = ada.get("ad", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            cost_micros = int(m.get("costMicros", 0))
            avg_cpc_micros = int(m.get("averageCpc", 0))

            ads.append({
                "ad_id": str(ad.get("id", "")),
                "ad_type": ad.get("type", ""),
                "ad_name": ad.get("name", ""),
                "final_urls": ad.get("finalUrls", []),
                "ad_status": ada.get("status", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
                "avg_cpc_dollars": round(avg_cpc_micros / 1_000_000, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(ads)} ads.")

        return {
            "ads": ads,
            "total": len(ads),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_ad_performance failed: {str(e)}")
        raise


@mcp.tool
def get_ad_group_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get ad group performance metrics."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching ad group performance for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            f"segments.date DURING {date_range}",
            "campaign.status != 'REMOVED'",
            "ad_group.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group.status,
                ad_group.cpc_bid_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc
            FROM ad_group
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        ad_groups = []
        for row in rows:
            m = row.get("metrics", {})
            ag = row.get("adGroup", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))
            bid_micros = int(ag.get("cpcBidMicros", 0))
            avg_cpc_micros = int(m.get("averageCpc", 0))

            ad_groups.append({
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
                "ad_group_status": ag.get("status", ""),
                "bid_dollars": round(bid_micros / 1_000_000, 4),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
                "avg_cpc_dollars": round(avg_cpc_micros / 1_000_000, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(ad_groups)} ad groups.")

        return {
            "ad_groups": ad_groups,
            "total": len(ad_groups),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_ad_group_performance failed: {str(e)}")
        raise


@mcp.tool
def get_geographic_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get performance breakdown by geographic location (country/region/city)."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching geographic report for customer {customer_id} ({date_range})...")

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
                geographic_view.country_criterion_id,
                geographic_view.location_type,
                segments.geo_target_city,
                segments.geo_target_region,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr
            FROM geographic_view
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        locations = []
        for row in rows:
            m = row.get("metrics", {})
            gv = row.get("geographicView", {})
            seg = row.get("segments", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))

            locations.append({
                "country_criterion_id": gv.get("countryCriterionId"),
                "location_type": gv.get("locationType", ""),
                "city": seg.get("geoTargetCity", ""),
                "region": seg.get("geoTargetRegion", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(locations)} geographic entries.")

        return {
            "locations": locations,
            "total": len(locations),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_geographic_report failed: {str(e)}")
        raise


@mcp.tool
def get_device_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get performance breakdown by device (mobile, desktop, tablet)."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching device report for customer {customer_id} ({date_range})...")

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
                segments.device,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc
            FROM campaign
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        devices = []
        for row in rows:
            m = row.get("metrics", {})
            seg = row.get("segments", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))
            avg_cpc_micros = int(m.get("averageCpc", 0))

            devices.append({
                "device": seg.get("device", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
                "avg_cpc_dollars": round(avg_cpc_micros / 1_000_000, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(devices)} device rows.")

        return {
            "devices": devices,
            "total": len(devices),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_device_report failed: {str(e)}")
        raise


@mcp.tool
def get_dayparting_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get performance breakdown by hour of day and day of week."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching dayparting report for customer {customer_id} ({date_range})...")

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
                segments.hour,
                segments.day_of_week,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.ctr
            FROM campaign
            WHERE {' AND '.join(where_clauses)}
            ORDER BY segments.day_of_week ASC, segments.hour ASC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        schedule = []
        for row in rows:
            m = row.get("metrics", {})
            seg = row.get("segments", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))

            schedule.append({
                "hour": seg.get("hour"),
                "day_of_week": seg.get("dayOfWeek", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(schedule)} dayparting rows.")

        return {
            "schedule": schedule,
            "total": len(schedule),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_dayparting_report failed: {str(e)}")
        raise


@mcp.tool
def get_landing_page_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get performance breakdown by landing page URL."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching landing page report for customer {customer_id} ({date_range})...")

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
                landing_page_view.unexpanded_final_url,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc
            FROM landing_page_view
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        landing_pages = []
        for row in rows:
            m = row.get("metrics", {})
            lpv = row.get("landingPageView", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))
            avg_cpc_micros = int(m.get("averageCpc", 0))

            landing_pages.append({
                "url": lpv.get("unexpandedFinalUrl", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": float(m.get("conversionsValue", 0)),
                "ctr": round(float(m.get("ctr", 0)) * 100, 4),
                "avg_cpc_dollars": round(avg_cpc_micros / 1_000_000, 4),
            })

        if ctx:
            ctx.info(f"Retrieved {len(landing_pages)} landing pages.")

        return {
            "landing_pages": landing_pages,
            "total": len(landing_pages),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_landing_page_report failed: {str(e)}")
        raise


@mcp.tool
def get_impression_share(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get Search impression share metrics per campaign (impression share, budget lost IS, rank lost IS)."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching impression share for customer {customer_id} ({date_range})...")

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
                campaign.advertising_channel_type,
                metrics.search_impression_share,
                metrics.search_budget_lost_impression_share,
                metrics.search_rank_lost_impression_share,
                metrics.search_top_impression_share,
                metrics.search_absolute_top_impression_share,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros
            FROM campaign
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.impressions DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        def _pct(val) -> Optional[float]:
            if val is None:
                return None
            try:
                return round(float(val) * 100, 2)
            except (TypeError, ValueError):
                return None

        campaigns = []
        for row in rows:
            m = row.get("metrics", {})
            camp = row.get("campaign", {})
            cost_micros = int(m.get("costMicros", 0))

            campaigns.append({
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "advertising_channel_type": camp.get("advertisingChannelType", ""),
                "search_impression_share": _pct(m.get("searchImpressionShare")),
                "search_budget_lost_impression_share": _pct(m.get("searchBudgetLostImpressionShare")),
                "search_rank_lost_impression_share": _pct(m.get("searchRankLostImpressionShare")),
                "search_top_impression_share": _pct(m.get("searchTopImpressionShare")),
                "search_absolute_top_impression_share": _pct(m.get("searchAbsoluteTopImpressionShare")),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
            })

        if ctx:
            ctx.info(f"Retrieved impression share for {len(campaigns)} campaigns.")

        return {
            "campaigns": campaigns,
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_impression_share failed: {str(e)}")
        raise


@mcp.tool
def get_wasted_spend(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    min_cost_dollars: float = 5.0,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Find keywords and campaigns spending money with zero conversions."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching wasted spend for customer {customer_id} ({date_range}), min=${min_cost_dollars}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        min_cost_micros = int(min_cost_dollars * 1_000_000)

        keyword_query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions
            FROM keyword_view
            WHERE segments.date DURING {date_range}
              AND metrics.conversions = 0
              AND metrics.cost_micros > 0
              AND ad_group_criterion.status != 'REMOVED'
              AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
            LIMIT 200
        """

        campaign_query = f"""
            SELECT
                campaign.id,
                campaign.name,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions
            FROM campaign
            WHERE segments.date DURING {date_range}
              AND metrics.conversions = 0
              AND metrics.cost_micros > 0
              AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
        """

        kw_result = execute_gaql(cid, keyword_query, mgr)
        camp_result = execute_gaql(cid, campaign_query, mgr)

        wasted_keywords = []
        for row in kw_result.get("results", []):
            m = row.get("metrics", {})
            cost_micros = int(m.get("costMicros", 0))
            if cost_micros < min_cost_micros:
                continue
            crit = row.get("adGroupCriterion", {})
            kw = crit.get("keyword", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})
            wasted_keywords.append({
                "keyword": kw.get("text", ""),
                "match_type": kw.get("matchType", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
            })

        wasted_campaigns = []
        for row in camp_result.get("results", []):
            m = row.get("metrics", {})
            cost_micros = int(m.get("costMicros", 0))
            if cost_micros < min_cost_micros:
                continue
            camp = row.get("campaign", {})
            wasted_campaigns.append({
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": round(cost_micros / 1_000_000, 2),
            })

        total_wasted = round(
            sum(k["cost_dollars"] for k in wasted_keywords)
            + sum(c["cost_dollars"] for c in wasted_campaigns),
            2,
        )

        if ctx:
            ctx.info(f"Found {len(wasted_keywords)} wasted keywords, {len(wasted_campaigns)} wasted campaigns. Total: ${total_wasted}")

        return {
            "wasted_keywords": wasted_keywords,
            "wasted_campaigns": wasted_campaigns,
            "total_wasted_dollars": total_wasted,
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_wasted_spend failed: {str(e)}")
        raise


@mcp.tool
def get_asset_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get individual asset (headline/description) performance labels within RSAs."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching asset performance for customer {customer_id} ({date_range})...")

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
                ad_group_ad_asset_view.field_type,
                ad_group_ad_asset_view.performance_label,
                ad_group_ad_asset_view.enabled,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                asset.id,
                asset.name,
                asset.type,
                asset.text_asset.text,
                metrics.impressions,
                metrics.clicks
            FROM ad_group_ad_asset_view
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.impressions DESC
            LIMIT 500
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        assets = []
        for row in rows:
            m = row.get("metrics", {})
            view = row.get("adGroupAdAssetView", {})
            asset = row.get("asset", {})
            text_asset = asset.get("textAsset", {})
            camp = row.get("campaign", {})
            ag = row.get("adGroup", {})

            assets.append({
                "asset_id": str(asset.get("id", "")),
                "asset_type": asset.get("type", ""),
                "asset_text": text_asset.get("text", ""),
                "field_type": view.get("fieldType", ""),
                "performance_label": view.get("performanceLabel", ""),
                "enabled": view.get("enabled", False),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "ad_group_id": str(ag.get("id", "")),
                "ad_group_name": ag.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
            })

        if ctx:
            ctx.info(f"Retrieved {len(assets)} assets.")

        return {
            "assets": assets,
            "total": len(assets),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_asset_performance failed: {str(e)}")
        raise


@mcp.tool
def get_pmax_asset_group_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get Performance Max asset group performance report."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    if ctx:
        ctx.info(f"Fetching PMax asset group report for customer {customer_id} ({date_range})...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        where_clauses = [
            f"segments.date DURING {date_range}",
            "campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
            "campaign.status != 'REMOVED'",
        ]
        if campaign_id:
            where_clauses.append(f"campaign.id = {campaign_id}")

        query = f"""
            SELECT
                asset_group.id,
                asset_group.name,
                asset_group.status,
                asset_group.final_urls,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM asset_group
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        asset_groups = []
        for row in rows:
            m = row.get("metrics", {})
            ag = row.get("assetGroup", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))
            cost_dollars = round(cost_micros / 1_000_000, 2)
            conversions_value = float(m.get("conversionsValue", 0))
            roas = round(conversions_value / cost_dollars, 4) if cost_dollars > 0 else 0.0

            asset_groups.append({
                "asset_group_id": str(ag.get("id", "")),
                "asset_group_name": ag.get("name", ""),
                "status": ag.get("status", ""),
                "final_urls": ag.get("finalUrls", []),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": cost_dollars,
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": conversions_value,
                "roas": roas,
            })

        if ctx:
            ctx.info(f"Retrieved {len(asset_groups)} PMax asset groups.")

        return {
            "asset_groups": asset_groups,
            "total": len(asset_groups),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_pmax_asset_group_report failed: {str(e)}")
        raise


@mcp.tool
def get_shopping_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    limit: int = 200,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get Shopping campaign product-level performance report."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    date_range = date_range.upper()
    if date_range not in VALID_DATE_RANGES:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(VALID_DATE_RANGES))}")

    limit = max(1, min(limit, 10000))

    if ctx:
        ctx.info(f"Fetching shopping performance for customer {customer_id} ({date_range})...")

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
                segments.product_item_id,
                segments.product_title,
                segments.product_brand,
                segments.product_type_l1,
                campaign.id,
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value
            FROM shopping_performance_view
            WHERE {' AND '.join(where_clauses)}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        products = []
        for row in rows:
            m = row.get("metrics", {})
            seg = row.get("segments", {})
            camp = row.get("campaign", {})

            cost_micros = int(m.get("costMicros", 0))
            cost_dollars = round(cost_micros / 1_000_000, 2)
            conversions_value = float(m.get("conversionsValue", 0))
            roas = round(conversions_value / cost_dollars, 4) if cost_dollars > 0 else 0.0

            products.append({
                "product_id": seg.get("productItemId", ""),
                "product_title": seg.get("productTitle", ""),
                "product_brand": seg.get("productBrand", ""),
                "product_type": seg.get("productTypeL1", ""),
                "campaign_id": str(camp.get("id", "")),
                "campaign_name": camp.get("name", ""),
                "impressions": int(m.get("impressions", 0)),
                "clicks": int(m.get("clicks", 0)),
                "cost_dollars": cost_dollars,
                "conversions": float(m.get("conversions", 0)),
                "conversions_value": conversions_value,
                "roas": roas,
            })

        if ctx:
            ctx.info(f"Retrieved {len(products)} products.")

        return {
            "products": products,
            "total": len(products),
            "date_range": date_range,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_shopping_performance failed: {str(e)}")
        raise
