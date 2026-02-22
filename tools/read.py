import requests
import logging
from typing import Any, Dict, List, Optional
from fastmcp import Context
from mcp_instance import mcp
from oauth.google_auth import (
    format_customer_id, get_headers_with_auto_token,
    execute_gaql, API_VERSION, GOOGLE_ADS_DEVELOPER_TOKEN,
    _make_request,
)

logger = logging.getLogger(__name__)


@mcp.tool
def run_gaql(
    customer_id: str,
    query: str,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Execute GAQL using the non-streaming search endpoint for consistent JSON parsing."""
    if ctx:
        ctx.info(f"Executing GAQL query for customer {customer_id}...")
        ctx.info(f"Query: {query}")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        # This will automatically trigger OAuth flow if needed
        result = execute_gaql(customer_id, query, manager_id)
        if ctx:
            ctx.info(f"GAQL query successful. Found {result['totalRows']} rows.")
        return result
    except Exception as e:
        if ctx:
            ctx.error(f"GAQL query failed: {str(e)}")
        raise


@mcp.tool
def get_account_performance(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    compare_prior_period: bool = True,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Get top-level account KPIs aggregated across all campaigns.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        date_range: Options: LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS, LAST_90_DAYS,
            THIS_MONTH, LAST_MONTH (default: LAST_30_DAYS)
        compare_prior_period: Also fetch the preceding period and show % change (default True)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Aggregated impressions, clicks, cost, conversions, ROAS, CTR, avg CPC,
        with period-over-period comparison when compare_prior_period=True
    """
    valid_ranges = {'LAST_7_DAYS', 'LAST_14_DAYS', 'LAST_30_DAYS', 'LAST_90_DAYS', 'THIS_MONTH', 'LAST_MONTH'}
    if date_range.upper() not in valid_ranges:
        raise ValueError(f"Invalid date_range. Must be one of: {', '.join(sorted(valid_ranges))}")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching account performance for customer {customer_id} ({date_range})...")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        def fetch(dr):
            q = f"""
                SELECT
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value,
                    metrics.all_conversions,
                    metrics.all_conversions_value
                FROM campaign
                WHERE segments.date DURING {dr}
                  AND campaign.status != 'REMOVED'
            """
            rows = execute_gaql(formatted_customer_id, q, mgr).get('results', [])
            totals = {'impressions': 0, 'clicks': 0, 'cost_micros': 0,
                      'conversions': 0.0, 'conversions_value': 0.0}
            for row in rows:
                m = row.get('metrics', {})
                totals['impressions'] += int(m.get('impressions', 0))
                totals['clicks'] += int(m.get('clicks', 0))
                totals['cost_micros'] += int(m.get('costMicros', 0))
                totals['conversions'] += float(m.get('conversions', 0))
                totals['conversions_value'] += float(m.get('conversionsValue', 0))
            return totals

        current = fetch(date_range.upper())
        cost = current['cost_micros']
        clicks = current['clicks']
        convs = current['conversions']
        conv_value = current['conversions_value']

        summary = {
            'date_range': date_range.upper(),
            'impressions': current['impressions'],
            'clicks': clicks,
            'cost_micros': cost,
            'cost_dollars': round(cost / 1_000_000, 2),
            'conversions': round(convs, 2),
            'conversions_value': round(conv_value, 2),
            'ctr': round((clicks / current['impressions'] * 100), 2) if current['impressions'] else 0,
            'avg_cpc_dollars': round((cost / clicks) / 1_000_000, 2) if clicks else 0,
            'cost_per_conversion': round((cost / 1_000_000) / convs, 2) if convs else 0,
            'roas': round(conv_value / (cost / 1_000_000), 2) if cost else 0,
            'customer_id': formatted_customer_id,
        }

        if compare_prior_period:
            range_to_days = {
                'LAST_7_DAYS': 7, 'LAST_14_DAYS': 14, 'LAST_30_DAYS': 30,
                'LAST_90_DAYS': 90, 'THIS_MONTH': 30, 'LAST_MONTH': 30
            }
            days = range_to_days.get(date_range.upper(), 30)
            from datetime import datetime, timedelta
            today = datetime.now().date()
            cur_end = today - timedelta(days=1)
            cur_start = cur_end - timedelta(days=days - 1)
            prior_end = cur_start - timedelta(days=1)
            prior_start = prior_end - timedelta(days=days - 1)

            prior_q = f"""
                SELECT metrics.impressions, metrics.clicks, metrics.cost_micros,
                       metrics.conversions, metrics.conversions_value
                FROM campaign
                WHERE segments.date BETWEEN '{prior_start}' AND '{prior_end}'
                  AND campaign.status != 'REMOVED'
            """
            prior_rows = execute_gaql(formatted_customer_id, prior_q, mgr).get('results', [])
            prior = {'impressions': 0, 'clicks': 0, 'cost_micros': 0, 'conversions': 0.0, 'conversions_value': 0.0}
            for row in prior_rows:
                m = row.get('metrics', {})
                prior['impressions'] += int(m.get('impressions', 0))
                prior['clicks'] += int(m.get('clicks', 0))
                prior['cost_micros'] += int(m.get('costMicros', 0))
                prior['conversions'] += float(m.get('conversions', 0))
                prior['conversions_value'] += float(m.get('conversionsValue', 0))

            def pct(cur, prv):
                if prv == 0:
                    return 100.0 if cur > 0 else 0.0
                return round((cur - prv) / abs(prv) * 100, 1)

            summary['prior_period'] = f"{prior_start} to {prior_end}"
            summary['changes'] = {
                'impressions_pct': pct(current['impressions'], prior['impressions']),
                'clicks_pct': pct(current['clicks'], prior['clicks']),
                'cost_pct': pct(current['cost_micros'], prior['cost_micros']),
                'conversions_pct': pct(current['conversions'], prior['conversions']),
                'conversions_value_pct': pct(current['conversions_value'], prior['conversions_value']),
            }

        if ctx:
            ctx.info(f"Account performance fetched. Cost: ${summary['cost_dollars']}, Conversions: {summary['conversions']}")

        return summary

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_quality_scores(
    customer_id: str,
    campaign_id: str = "",
    min_impressions: int = 0,
    limit: int = 500,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Get Quality Score breakdown for keywords.

    Returns overall QS plus the three sub-components: expected CTR,
    ad relevance, and landing page experience.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: Optional campaign ID to filter to a single campaign
        min_impressions: Only include keywords with at least this many impressions
            in the last 30 days (default 0 = all keywords with a QS score)
        limit: Max keywords to return (default 500)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Keywords sorted by Quality Score ascending (worst first) with sub-scores
    """
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching Quality Scores for customer {customer_id}...")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        limit = max(1, min(limit, 10000))

        campaign_filter = f"AND campaign.id = {campaign_id.strip()}" if campaign_id else ""
        impressions_filter = (
            f"AND metrics.impressions >= {min_impressions}" if min_impressions > 0 else ""
        )

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.criterion_id,
                ad_group_criterion.quality_info.quality_score,
                ad_group_criterion.quality_info.creative_quality_score,
                ad_group_criterion.quality_info.post_click_quality_score,
                ad_group_criterion.quality_info.search_predicted_ctr,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros
            FROM keyword_view
            WHERE segments.date DURING LAST_30_DAYS
              AND ad_group_criterion.status != 'REMOVED'
              AND ad_group.status = 'ENABLED'
              AND campaign.status = 'ENABLED'
              {campaign_filter}
              {impressions_filter}
            ORDER BY ad_group_criterion.quality_info.quality_score ASC
            LIMIT {limit}
        """

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        if ctx:
            ctx.info(f"Found {len(rows)} keyword(s) with Quality Score data.")

        keywords = []
        for row in rows:
            c = row.get('campaign', {})
            ag = row.get('adGroup', {})
            agc = row.get('adGroupCriterion', {})
            qi = agc.get('qualityInfo', {})
            m = row.get('metrics', {})
            cost_micros = int(m.get('costMicros', 0))
            keywords.append({
                'keyword': agc.get('keyword', {}).get('text', ''),
                'match_type': agc.get('keyword', {}).get('matchType', ''),
                'criterion_id': agc.get('criterionId', ''),
                'quality_score': qi.get('qualityScore'),
                'expected_ctr': qi.get('searchPredictedCtr', ''),
                'ad_relevance': qi.get('creativeQualityScore', ''),
                'landing_page_experience': qi.get('postClickQualityScore', ''),
                'campaign_id': c.get('id', ''),
                'campaign_name': c.get('name', ''),
                'ad_group_id': ag.get('id', ''),
                'ad_group_name': ag.get('name', ''),
                'impressions': int(m.get('impressions', 0)),
                'clicks': int(m.get('clicks', 0)),
                'cost_dollars': round(cost_micros / 1_000_000, 2),
            })

        return {
            'keywords': keywords,
            'total': len(keywords),
            'customer_id': formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_disapproved_ads(
    customer_id: str,
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Find all disapproved or limited ads and their policy violation reasons.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: Optional campaign ID to filter to a single campaign
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        List of disapproved ads with policy topics, campaign, and ad group context
    """
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Searching for disapproved ads for customer {customer_id}...")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        campaign_filter = f"AND campaign.id = {campaign_id.strip()}" if campaign_id else ""

        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.ad.final_urls,
                ad_group_ad.policy_summary.approval_status,
                ad_group_ad.policy_summary.review_status,
                ad_group_ad.policy_summary.policy_topic_entries
            FROM ad_group_ad
            WHERE ad_group_ad.policy_summary.approval_status IN ('DISAPPROVED', 'AREA_OF_INTEREST_ONLY')
              AND ad_group_ad.status != 'REMOVED'
              AND campaign.status != 'REMOVED'
              {campaign_filter}
        """

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        if ctx:
            ctx.info(f"Found {len(rows)} disapproved ad(s).")

        ads = []
        for row in rows:
            c = row.get('campaign', {})
            ag = row.get('adGroup', {})
            ad = row.get('adGroupAd', {}).get('ad', {})
            ps = row.get('adGroupAd', {}).get('policySummary', {})
            topics = ps.get('policyTopicEntries', [])
            ads.append({
                'ad_id': ad.get('id', ''),
                'ad_type': ad.get('type', ''),
                'final_urls': ad.get('finalUrls', []),
                'approval_status': ps.get('approvalStatus', ''),
                'review_status': ps.get('reviewStatus', ''),
                'policy_topics': [
                    {
                        'topic': t.get('topic', ''),
                        'type': t.get('type', ''),
                        'evidences': t.get('evidences', [])
                    }
                    for t in topics
                ],
                'campaign_id': c.get('id', ''),
                'campaign_name': c.get('name', ''),
                'ad_group_id': ag.get('id', ''),
                'ad_group_name': ag.get('name', ''),
            })

        return {
            'disapproved_ads': ads,
            'total': len(ads),
            'customer_id': formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_auction_insights(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Get auction insights showing how your ads compare to competitors.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        date_range: Options: LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS, LAST_90_DAYS,
            THIS_MONTH, LAST_MONTH (default: LAST_30_DAYS)
        campaign_id: Optional campaign ID to scope insights to one campaign
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Per-competitor metrics: impression share, overlap rate, position above rate,
        top-of-page rate, abs top-of-page rate, outranking share
    """
    valid_ranges = {'LAST_7_DAYS', 'LAST_14_DAYS', 'LAST_30_DAYS', 'LAST_90_DAYS', 'THIS_MONTH', 'LAST_MONTH'}
    if date_range.upper() not in valid_ranges:
        raise ValueError(f"Invalid date_range. Must be one of: {', '.join(sorted(valid_ranges))}")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching auction insights for customer {customer_id} ({date_range})...")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        campaign_filter = f"AND campaign.id = {campaign_id.strip()}" if campaign_id else ""

        query = f"""
            SELECT
                auction_insight_summary.domain,
                auction_insight_summary.impression_share,
                auction_insight_summary.overlap_rate,
                auction_insight_summary.outranking_share,
                auction_insight_summary.position_above_rate,
                auction_insight_summary.top_of_page_rate,
                auction_insight_summary.abs_top_of_page_rate
            FROM auction_insight_summary
            WHERE segments.date DURING {date_range.upper()}
            {campaign_filter}
        """

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        if ctx:
            ctx.info(f"Found {len(rows)} competitor(s) in auction insights.")

        competitors = []
        for row in rows:
            ai = row.get('auctionInsightSummary', {})
            competitors.append({
                'domain': ai.get('domain', ''),
                'impression_share': ai.get('impressionShare'),
                'overlap_rate': ai.get('overlapRate'),
                'outranking_share': ai.get('outrankingShare'),
                'position_above_rate': ai.get('positionAboveRate'),
                'top_of_page_rate': ai.get('topOfPageRate'),
                'abs_top_of_page_rate': ai.get('absTopOfPageRate'),
            })

        competitors.sort(key=lambda x: x.get('impression_share') or 0, reverse=True)

        return {
            'competitors': competitors,
            'total': len(competitors),
            'date_range': date_range.upper(),
            'campaign_filter': campaign_id or None,
            'customer_id': formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_performance_anomalies(
    customer_id: str,
    current_days: int = 7,
    threshold_pct: float = 20.0,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Detect campaigns with significant performance changes vs the prior period.

    Compares the last N days against the preceding N days and flags campaigns
    whose clicks, impressions, cost, or conversions changed by more than the threshold.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        current_days: Number of days in the comparison window (default 7)
        threshold_pct: Minimum % change to flag a metric as anomalous (default 20.0)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Campaigns with anomalies, showing which metrics changed and by how much,
        sorted by largest absolute change first
    """
    if current_days < 1 or current_days > 90:
        raise ValueError("current_days must be between 1 and 90.")

    if ctx:
        ctx.info(f"Comparing last {current_days} days vs prior {current_days} days for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        from datetime import datetime, timedelta

        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        today = datetime.now().date()
        current_end = today - timedelta(days=1)
        current_start = current_end - timedelta(days=current_days - 1)
        prior_end = current_start - timedelta(days=1)
        prior_start = prior_end - timedelta(days=current_days - 1)

        def fetch_metrics(start, end):
            q = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions
                FROM campaign
                WHERE segments.date BETWEEN '{start}' AND '{end}'
                  AND campaign.status = 'ENABLED'
            """
            r = execute_gaql(formatted_customer_id, q, mgr)
            by_id = {}
            for row in r.get('results', []):
                c = row.get('campaign', {})
                m = row.get('metrics', {})
                cid = str(c.get('id', ''))
                if cid not in by_id:
                    by_id[cid] = {
                        'campaign_name': c.get('name', ''),
                        'impressions': 0, 'clicks': 0, 'cost_micros': 0, 'conversions': 0.0
                    }
                by_id[cid]['impressions'] += int(m.get('impressions', 0))
                by_id[cid]['clicks'] += int(m.get('clicks', 0))
                by_id[cid]['cost_micros'] += int(m.get('costMicros', 0))
                by_id[cid]['conversions'] += float(m.get('conversions', 0))
            return by_id

        current = fetch_metrics(current_start, current_end)
        prior = fetch_metrics(prior_start, prior_end)

        if ctx:
            ctx.info(f"Analysing {len(current)} active campaign(s)...")

        anomalies = []
        for cid, cur in current.items():
            pri = prior.get(cid, {})
            campaign_anomalies = {}
            for metric in ('impressions', 'clicks', 'cost_micros', 'conversions'):
                cur_val = cur.get(metric, 0)
                pri_val = pri.get(metric, 0)
                if pri_val == 0:
                    if cur_val > 0:
                        pct_change = 100.0
                    else:
                        continue
                else:
                    pct_change = ((cur_val - pri_val) / abs(pri_val)) * 100
                if abs(pct_change) >= threshold_pct:
                    campaign_anomalies[metric] = {
                        'current': cur_val,
                        'prior': pri_val,
                        'change_pct': round(pct_change, 1)
                    }
            if campaign_anomalies:
                anomalies.append({
                    'campaign_id': cid,
                    'campaign_name': cur.get('campaign_name', ''),
                    'anomalies': campaign_anomalies
                })

        anomalies.sort(
            key=lambda x: max(abs(v['change_pct']) for v in x['anomalies'].values()),
            reverse=True
        )

        if ctx:
            ctx.info(f"Found {len(anomalies)} campaign(s) with anomalies (threshold: {threshold_pct}%).")

        return {
            'anomalies': anomalies,
            'campaigns_with_anomalies': len(anomalies),
            'current_period': f"{current_start} to {current_end}",
            'prior_period': f"{prior_start} to {prior_end}",
            'threshold_pct': threshold_pct,
            'customer_id': formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_search_terms_report(
    customer_id: str,
    date_range: str = "LAST_30_DAYS",
    campaign_id: str = "",
    min_impressions: int = 0,
    limit: int = 500,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Get a search terms report showing which actual user queries triggered your ads.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        date_range: Date range for the report. Options: LAST_7_DAYS, LAST_14_DAYS,
            LAST_30_DAYS, LAST_90_DAYS, THIS_MONTH, LAST_MONTH (default: LAST_30_DAYS)
        campaign_id: Optional campaign ID to filter results to a single campaign
        min_impressions: Minimum impressions threshold to include a term (default 0 = all)
        limit: Maximum number of search terms to return (default 500, max 10000)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        List of search terms with performance metrics, sorted by cost descending
    """
    valid_ranges = {'LAST_7_DAYS', 'LAST_14_DAYS', 'LAST_30_DAYS', 'LAST_90_DAYS', 'THIS_MONTH', 'LAST_MONTH'}
    if date_range.upper() not in valid_ranges:
        raise ValueError(f"Invalid date_range '{date_range}'. Must be one of: {', '.join(sorted(valid_ranges))}")

    if ctx:
        ctx.info(f"Fetching search terms report for customer {customer_id} ({date_range})...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        limit = max(1, min(limit, 10000))

        campaign_filter = f"AND campaign.id = {campaign_id.strip()}" if campaign_id else ""
        impressions_filter = f"AND metrics.impressions >= {min_impressions}" if min_impressions > 0 else ""

        query = f"""
            SELECT
                search_term_view.search_term,
                search_term_view.status,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.ctr,
                metrics.average_cpc
            FROM search_term_view
            WHERE segments.date DURING {date_range.upper()}
            {campaign_filter}
            {impressions_filter}
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        if ctx:
            ctx.info(f"Found {len(rows)} search terms.")

        terms = []
        for row in rows:
            stv = row.get('searchTermView', {})
            campaign = row.get('campaign', {})
            ad_group = row.get('adGroup', {})
            metrics = row.get('metrics', {})
            cost_micros = int(metrics.get('costMicros', 0))
            avg_cpc_micros = int(metrics.get('averageCpc', 0))
            terms.append({
                'search_term': stv.get('searchTerm', ''),
                'status': stv.get('status', ''),
                'campaign_id': campaign.get('id', ''),
                'campaign_name': campaign.get('name', ''),
                'ad_group_id': ad_group.get('id', ''),
                'ad_group_name': ad_group.get('name', ''),
                'impressions': int(metrics.get('impressions', 0)),
                'clicks': int(metrics.get('clicks', 0)),
                'cost_micros': cost_micros,
                'cost_dollars': round(cost_micros / 1_000_000, 2),
                'conversions': float(metrics.get('conversions', 0)),
                'conversions_value': float(metrics.get('conversionsValue', 0)),
                'ctr': float(metrics.get('ctr', 0)),
                'average_cpc_micros': avg_cpc_micros,
                'average_cpc_dollars': round(avg_cpc_micros / 1_000_000, 2)
            })

        return {
            'search_terms': terms,
            'total_terms': len(terms),
            'date_range': date_range.upper(),
            'customer_id': formatted_customer_id,
            'campaign_filter': campaign_id or None
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_campaign_details(
    customer_id: str,
    campaign_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get full settings for a single campaign without writing GAQL.

    Returns bidding strategy, budget, network settings, status, dates,
    and targeting type in one call.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to inspect
        manager_id: Manager ID if the account is accessed through an MCC
    """
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")
    if ctx:
        ctx.info(f"Fetching details for campaign {campaign_id}...")
    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.bidding_strategy_type,
                campaign.target_cpa.target_cpa_micros,
                campaign.target_roas.target_roas,
                campaign.maximize_conversions.target_cpa_micros,
                campaign.maximize_conversion_value.target_roas,
                campaign.manual_cpc.enhanced_cpc_enabled,
                campaign.start_date,
                campaign.end_date,
                campaign.network_settings.target_google_search,
                campaign.network_settings.target_search_network,
                campaign.network_settings.target_content_network,
                campaign_budget.amount_micros,
                campaign_budget.name,
                campaign_budget.delivery_method
            FROM campaign
            WHERE campaign.id = {campaign_id.strip()}
        """
        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])
        if not rows:
            raise Exception(f"No campaign found with ID {campaign_id}.")
        row = rows[0]
        c = row.get('campaign', {})
        b = row.get('campaignBudget', {})
        budget_micros = int(b.get('amountMicros', 0))
        details = {
            'campaign_id': c.get('id', ''),
            'name': c.get('name', ''),
            'status': c.get('status', ''),
            'advertising_channel_type': c.get('advertisingChannelType', ''),
            'bidding_strategy_type': c.get('biddingStrategyType', ''),
            'start_date': c.get('startDate', ''),
            'end_date': c.get('endDate', ''),
            'network_settings': {
                'google_search': c.get('networkSettings', {}).get('targetGoogleSearch'),
                'search_network': c.get('networkSettings', {}).get('targetSearchNetwork'),
                'display_network': c.get('networkSettings', {}).get('targetContentNetwork'),
            },
            'budget': {
                'name': b.get('name', ''),
                'amount_micros': budget_micros,
                'amount_dollars': round(budget_micros / 1_000_000, 2),
                'delivery_method': b.get('deliveryMethod', ''),
            },
            'customer_id': formatted_customer_id,
        }
        # Include bidding strategy details
        if c.get('targetCpa', {}).get('targetCpaMicros'):
            details['target_cpa_micros'] = int(c['targetCpa']['targetCpaMicros'])
            details['target_cpa_dollars'] = round(details['target_cpa_micros'] / 1_000_000, 2)
        if c.get('targetRoas', {}).get('targetRoas'):
            details['target_roas'] = c['targetRoas']['targetRoas']
        return details
    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_budget_pacing(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Check how campaigns are tracking against their daily budgets today.

    Compares today's spend so far against the expected spend based on how much
    of the day has elapsed. Flags campaigns that are significantly over- or under-pacing.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Per-campaign pacing status with spend, budget, and pacing percentage
    """
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")
    if ctx:
        ctx.info(f"Fetching budget pacing for customer {customer_id}...")
    try:
        from datetime import datetime
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        query = """
            SELECT
                campaign.id,
                campaign.name,
                campaign_budget.amount_micros,
                metrics.cost_micros
            FROM campaign
            WHERE segments.date DURING TODAY
              AND campaign.status = 'ENABLED'
        """
        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        now = datetime.now()
        elapsed_fraction = (now.hour * 3600 + now.minute * 60 + now.second) / 86400
        elapsed_fraction = max(elapsed_fraction, 0.01)  # avoid division by zero early AM

        campaigns = []
        for row in rows:
            c = row.get('campaign', {})
            b = row.get('campaignBudget', {})
            m = row.get('metrics', {})
            budget_micros = int(b.get('amountMicros', 0))
            cost_micros = int(m.get('costMicros', 0))
            expected_micros = budget_micros * elapsed_fraction
            pacing_pct = round((cost_micros / expected_micros * 100), 1) if expected_micros > 0 else 0
            if pacing_pct >= 120:
                status = 'OVERPACING'
            elif pacing_pct <= 80:
                status = 'UNDERPACING'
            else:
                status = 'ON_TRACK'
            campaigns.append({
                'campaign_id': str(c.get('id', '')),
                'campaign_name': c.get('name', ''),
                'budget_micros': budget_micros,
                'budget_dollars': round(budget_micros / 1_000_000, 2),
                'spend_today_micros': cost_micros,
                'spend_today_dollars': round(cost_micros / 1_000_000, 2),
                'expected_spend_dollars': round(expected_micros / 1_000_000, 2),
                'pacing_pct': pacing_pct,
                'pacing_status': status,
            })

        campaigns.sort(key=lambda x: abs(x['pacing_pct'] - 100), reverse=True)

        overpacing = [c for c in campaigns if c['pacing_status'] == 'OVERPACING']
        underpacing = [c for c in campaigns if c['pacing_status'] == 'UNDERPACING']

        if ctx:
            ctx.info(f"Pacing check: {len(overpacing)} overpacing, {len(underpacing)} underpacing, {len(campaigns) - len(overpacing) - len(underpacing)} on track.")

        return {
            'campaigns': campaigns,
            'total_campaigns': len(campaigns),
            'overpacing_count': len(overpacing),
            'underpacing_count': len(underpacing),
            'elapsed_day_pct': round(elapsed_fraction * 100, 1),
            'customer_id': formatted_customer_id,
        }
    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise
