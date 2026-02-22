import requests
import logging
from datetime import datetime
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
def run_keyword_planner(
    customer_id: str,
    keywords: List[str],
    manager_id: str = "",
    page_url: Optional[str] = None,
    language_id: int = 1000,
    geo_target_id: int = 2840,
    page_size: int = 100,
    start_year: Optional[int] = None,
    start_month: Optional[str] = None,
    end_year: Optional[int] = None,
    end_month: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """Generate keyword ideas using Google Ads KeywordPlanIdeaService.

    This tool allows you to generate keyword ideas based on seed keywords or a page URL.
    You can specify targeting parameters such as language, location, and network to refine your keyword suggestions.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        keywords: A list of seed keywords to generate ideas from
        manager_id: Manager ID if access type is 'managed'
        page_url: Optional page URL related to your business to generate ideas from
        language_id: Google Ads language constant ID (default 1000 = English).
            Common values: 1000=English, 1003=Spanish, 1004=French, 1005=German,
            1006=Italian, 1009=Portuguese, 1010=Dutch, 1011=Japanese, 1012=Arabic,
            1014=Korean, 1018=Polish, 1019=Russian, 1020=Turkish, 1023=Chinese (Simplified)
        geo_target_id: Google Ads geo target constant ID (default 2840 = United States).
            Common values: 2840=US, 2826=UK, 2124=Canada, 2036=Australia,
            2276=Germany, 2250=France, 2724=Spain, 2380=Italy, 2076=Brazil,
            2484=Mexico, 2356=India, 2392=Japan, 2410=South Korea, 2076=Brazil
        page_size: Number of keyword ideas to return (default 100, max 10000)
        start_year: Optional start year for historical data (defaults to previous year)
        start_month: Optional start month for historical data (defaults to JANUARY)
        end_year: Optional end year for historical data (defaults to current year)
        end_month: Optional end month for historical data (defaults to current month)

    Returns:
        A list of keyword ideas with associated metrics

    Note:
        - At least one of 'keywords' or 'page_url' must be provided
        - Ensure that the 'customer_id' is formatted as a string, even if it appears numeric
        - Valid months: JANUARY, FEBRUARY, MARCH, APRIL, MAY, JUNE, JULY, AUGUST, SEPTEMBER, OCTOBER, NOVEMBER, DECEMBER
    """
    if ctx:
        ctx.info(f"Generating keyword ideas for customer {customer_id}...")
        if keywords:
            ctx.info(f"Seed keywords: {', '.join(keywords)}")
        if page_url:
            ctx.info(f"Page URL: {page_url}")
        ctx.info(f"Language ID: {language_id}, Geo target ID: {geo_target_id}, Page size: {page_size}")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if (not keywords or len(keywords) == 0) and not page_url:
        raise ValueError("At least one of keywords or page URL is required, but neither was specified.")

    page_size = max(1, min(page_size, 10000))

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}:generateKeywordIdeas"

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.strftime('%B').upper()

        valid_months = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
                        'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']

        start_year_final = start_year or (current_year - 1)
        start_month_final = start_month.upper() if start_month and start_month.upper() in valid_months else 'JANUARY'
        end_year_final = end_year or current_year
        end_month_final = end_month.upper() if end_month and end_month.upper() in valid_months else current_month

        request_body = {
            'language': f'languageConstants/{language_id}',
            'geoTargetConstants': [f'geoTargetConstants/{geo_target_id}'],
            'keywordPlanNetwork': 'GOOGLE_SEARCH_AND_PARTNERS',
            'includeAdultKeywords': False,
            'pageSize': page_size,
            'historicalMetricsOptions': {
                'yearMonthRange': {
                    'start': {'year': start_year_final, 'month': start_month_final},
                    'end': {'year': end_year_final, 'month': end_month_final}
                }
            }
        }

        if (not keywords or len(keywords) == 0) and page_url:
            request_body['urlSeed'] = {'url': page_url}
        elif keywords and len(keywords) > 0 and not page_url:
            request_body['keywordSeed'] = {'keywords': keywords}
        elif keywords and len(keywords) > 0 and page_url:
            request_body['keywordAndUrlSeed'] = {'url': page_url, 'keywords': keywords}

        response = _make_request(requests.post, url, headers, json_body=request_body)

        if not response.ok:
            if ctx:
                ctx.error(f"Keyword planner request failed: {response.status_code} {response.reason}")
            raise Exception(f"Error executing request: {response.status_code} {response.reason} - {response.text}")

        results = response.json()

        if 'results' not in results or not results['results']:
            message = (
                f"No keyword ideas found for the provided inputs.\n\n"
                f"Keywords: {', '.join(keywords) if keywords else 'None'}\n"
                f"Page URL: {page_url or 'None'}\n"
                f"Account: {formatted_customer_id}"
            )
            if ctx:
                ctx.info(message)
            return {
                "message": message,
                "keywords": keywords or [],
                "page_url": page_url,
                "date_range": f"{start_month_final} {start_year_final} to {end_month_final} {end_year_final}"
            }

        formatted_results = []
        for result in results['results']:
            keyword_idea = result.get('keywordIdeaMetrics', {})
            formatted_results.append({
                'keyword': result.get('text', 'N/A'),
                'avg_monthly_searches': keyword_idea.get('avgMonthlySearches', 'N/A'),
                'competition': keyword_idea.get('competition', 'N/A'),
                'competition_index': keyword_idea.get('competitionIndex', 'N/A'),
                'low_top_of_page_bid_micros': keyword_idea.get('lowTopOfPageBidMicros', 'N/A'),
                'high_top_of_page_bid_micros': keyword_idea.get('highTopOfPageBidMicros', 'N/A')
            })

        if ctx:
            ctx.info(f"Found {len(formatted_results)} keyword ideas.")

        return {
            "keyword_ideas": formatted_results,
            "total_ideas": len(formatted_results),
            "input_keywords": keywords or [],
            "input_page_url": page_url,
            "language_id": language_id,
            "geo_target_id": geo_target_id,
            "date_range": f"{start_month_final} {start_year_final} to {end_month_final} {end_year_final}"
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def set_campaign_status(
    customer_id: str,
    campaign_ids: List[str],
    status: str,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Pause or enable one or more campaigns.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_ids: List of campaign IDs to update (e.g. ["12345678", "98765432"])
        status: New status for the campaigns. Must be 'ENABLED' or 'PAUSED'
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        A summary of which campaigns were updated successfully and any failures
    """
    status = status.upper()
    if status not in ('ENABLED', 'PAUSED'):
        raise ValueError(f"Invalid status '{status}'. Must be 'ENABLED' or 'PAUSED'.")
    if not campaign_ids:
        raise ValueError("campaign_ids must not be empty.")

    if ctx:
        ctx.info(f"Setting {len(campaign_ids)} campaign(s) to {status} for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaigns:mutate"
        operations = [
            {
                "update": {
                    "resourceName": f"customers/{formatted_customer_id}/campaigns/{cid.strip()}",
                    "status": status
                },
                "updateMask": "status"
            }
            for cid in campaign_ids
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            if ctx:
                ctx.error(f"Campaign mutate request failed: {response.status_code} {response.reason}")
            raise Exception(f"Error mutating campaigns: {response.status_code} {response.reason} - {response.text}")

        updated = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully updated {len(updated)} campaign(s) to {status}.")

        return {
            "status_set": status,
            "campaigns_updated": len(updated),
            "updated_resource_names": updated,
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_keywords(
    customer_id: str,
    ad_group_id: str,
    keywords: List[Dict[str, str]],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add positive keywords to an ad group.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        ad_group_id: The ad group ID to add keywords to
        keywords: List of keyword dicts. Each must have 'text' and 'match_type'.
            match_type options: 'BROAD', 'PHRASE', 'EXACT'
            Example: [{"text": "running shoes", "match_type": "PHRASE"}, {"text": "buy sneakers", "match_type": "EXACT"}]
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of keywords added, including resource names
    """
    if not keywords:
        raise ValueError("keywords list must not be empty.")

    valid_match_types = {'BROAD', 'PHRASE', 'EXACT'}
    for kw in keywords:
        if 'text' not in kw or 'match_type' not in kw:
            raise ValueError("Each keyword must have 'text' and 'match_type' fields.")
        if kw['match_type'].upper() not in valid_match_types:
            raise ValueError(f"Invalid match_type '{kw['match_type']}'. Must be one of: BROAD, PHRASE, EXACT")

    if ctx:
        ctx.info(f"Adding {len(keywords)} keyword(s) to ad group {ad_group_id} for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupCriteria:mutate"
        operations = [
            {
                "create": {
                    "adGroup": f"customers/{formatted_customer_id}/adGroups/{ad_group_id.strip()}",
                    "status": "ENABLED",
                    "keyword": {
                        "text": kw['text'],
                        "matchType": kw['match_type'].upper()
                    }
                }
            }
            for kw in keywords
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error adding keywords: {response.status_code} {response.reason} - {response.text}")

        created = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(created)} keyword(s).")

        return {
            "keywords_added": len(created),
            "ad_group_id": ad_group_id,
            "customer_id": formatted_customer_id,
            "created_resource_names": created
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_negative_keywords(
    customer_id: str,
    keywords: List[Dict[str, str]],
    campaign_id: str = "",
    ad_group_id: str = "",
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add negative keywords at the campaign or ad group level.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        keywords: List of keyword dicts. Each must have 'text' and 'match_type'.
            match_type options: 'BROAD', 'PHRASE', 'EXACT'
            Example: [{"text": "free", "match_type": "BROAD"}]
        campaign_id: Campaign ID for campaign-level negatives (provide this OR ad_group_id)
        ad_group_id: Ad group ID for ad-group-level negatives (provide this OR campaign_id)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of negative keywords added
    """
    if not keywords:
        raise ValueError("keywords list must not be empty.")
    if not campaign_id and not ad_group_id:
        raise ValueError("Provide either campaign_id (campaign-level) or ad_group_id (ad-group-level).")
    if campaign_id and ad_group_id:
        raise ValueError("Provide either campaign_id or ad_group_id, not both.")

    valid_match_types = {'BROAD', 'PHRASE', 'EXACT'}
    for kw in keywords:
        if 'text' not in kw or 'match_type' not in kw:
            raise ValueError("Each keyword must have 'text' and 'match_type' fields.")
        if kw['match_type'].upper() not in valid_match_types:
            raise ValueError(f"Invalid match_type '{kw['match_type']}'. Must be one of: BROAD, PHRASE, EXACT")

    level = "campaign" if campaign_id else "ad group"
    if ctx:
        ctx.info(f"Adding {len(keywords)} negative keyword(s) at {level} level for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        if campaign_id:
            url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"
            operations = [
                {
                    "create": {
                        "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                        "negative": True,
                        "keyword": {"text": kw['text'], "matchType": kw['match_type'].upper()}
                    }
                }
                for kw in keywords
            ]
        else:
            url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupCriteria:mutate"
            operations = [
                {
                    "create": {
                        "adGroup": f"customers/{formatted_customer_id}/adGroups/{ad_group_id.strip()}",
                        "negative": True,
                        "keyword": {"text": kw['text'], "matchType": kw['match_type'].upper()}
                    }
                }
                for kw in keywords
            ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error adding negative keywords: {response.status_code} {response.reason} - {response.text}")

        created = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(created)} negative keyword(s) at {level} level.")

        return {
            "negative_keywords_added": len(created),
            "level": level,
            "campaign_id": campaign_id or None,
            "ad_group_id": ad_group_id or None,
            "customer_id": formatted_customer_id,
            "created_resource_names": created
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def update_campaign_budget(
    customer_id: str,
    campaign_id: str,
    new_daily_budget_micros: int,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Update the daily budget for a campaign.

    Automatically looks up the campaign's budget resource and updates it.
    Note: if the budget is shared across multiple campaigns, all of them will be affected.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID whose budget to update
        new_daily_budget_micros: New daily budget in micros (1,000,000 micros = $1.00).
            Example: 50000000 = $50/day
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        The updated budget resource name and new daily amount
    """
    if new_daily_budget_micros <= 0:
        raise ValueError("new_daily_budget_micros must be a positive integer.")

    if ctx:
        ctx.info(f"Looking up budget for campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = f"SELECT campaign.campaign_budget FROM campaign WHERE campaign.id = {campaign_id.strip()}"
        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])
        if not rows:
            raise Exception(f"No campaign found with ID {campaign_id} for customer {formatted_customer_id}.")

        budget_resource = rows[0].get('campaign', {}).get('campaignBudget', '')
        if not budget_resource:
            raise Exception(f"Could not retrieve budget resource name for campaign {campaign_id}.")

        if ctx:
            ctx.info(f"Found budget: {budget_resource}. Updating to {new_daily_budget_micros} micros (${round(new_daily_budget_micros / 1_000_000, 2)}/day)...")

        headers = get_headers_with_auto_token()
        if manager_id:
            headers['login-customer-id'] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignBudgets:mutate"
        operations = [
            {
                "update": {
                    "resourceName": budget_resource,
                    "amountMicros": str(new_daily_budget_micros)
                },
                "updateMask": "amountMicros"
            }
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error updating budget: {response.status_code} {response.reason} - {response.text}")

        updated = response.json().get('results', [{}])[0].get('resourceName', budget_resource)

        if ctx:
            ctx.info("Budget updated successfully.")

        return {
            "budget_updated": updated,
            "campaign_id": campaign_id,
            "new_daily_budget_micros": new_daily_budget_micros,
            "new_daily_budget_dollars": round(new_daily_budget_micros / 1_000_000, 2),
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def create_responsive_search_ad(
    customer_id: str,
    ad_group_id: str,
    final_url: str,
    headlines: List[str],
    descriptions: List[str],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Create a Responsive Search Ad (RSA) in an ad group.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        ad_group_id: The ad group ID to create the ad in
        final_url: The landing page URL (e.g. "https://example.com/page")
        headlines: List of 3-15 headline strings. Each must be 30 characters or fewer.
        descriptions: List of 2-4 description strings. Each must be 90 characters or fewer.
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        The resource name of the created ad
    """
    if len(headlines) < 3 or len(headlines) > 15:
        raise ValueError(f"RSA requires 3-15 headlines, got {len(headlines)}.")
    if len(descriptions) < 2 or len(descriptions) > 4:
        raise ValueError(f"RSA requires 2-4 descriptions, got {len(descriptions)}.")
    for h in headlines:
        if len(h) > 30:
            raise ValueError(f"Headline too long (max 30 chars): '{h}' ({len(h)} chars)")
    for d in descriptions:
        if len(d) > 90:
            raise ValueError(f"Description too long (max 90 chars): '{d}' ({len(d)} chars)")

    if ctx:
        ctx.info(f"Creating RSA in ad group {ad_group_id} for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupAds:mutate"
        operation = {
            "create": {
                "adGroup": f"customers/{formatted_customer_id}/adGroups/{ad_group_id.strip()}",
                "status": "ENABLED",
                "ad": {
                    "finalUrls": [final_url],
                    "responsiveSearchAd": {
                        "headlines": [{"text": h} for h in headlines],
                        "descriptions": [{"text": d} for d in descriptions]
                    }
                }
            }
        }

        response = _make_request(requests.post, url, headers, json_body={"operations": [operation]})

        if not response.ok:
            raise Exception(f"Error creating RSA: {response.status_code} {response.reason} - {response.text}")

        resource_name = response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"RSA created successfully: {resource_name}")

        return {
            "ad_created": resource_name,
            "ad_group_id": ad_group_id,
            "final_url": final_url,
            "headline_count": len(headlines),
            "description_count": len(descriptions),
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def update_keyword_bid(
    customer_id: str,
    keywords: List[Dict[str, Any]],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Update the CPC bid for one or more keywords.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        keywords: List of keyword dicts. Each must have:
            - 'ad_group_id': The ad group ID
            - 'criterion_id': The keyword criterion ID
              (get via run_gaql: SELECT ad_group_criterion.criterion_id FROM ad_group_criterion)
            - 'cpc_bid_micros': New CPC bid in micros (e.g. 1000000 = $1.00)
            Example: [{"ad_group_id": "111", "criterion_id": "222", "cpc_bid_micros": 1500000}]
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of updated keyword bids
    """
    if not keywords:
        raise ValueError("keywords list must not be empty.")
    for kw in keywords:
        for field in ('ad_group_id', 'criterion_id', 'cpc_bid_micros'):
            if field not in kw:
                raise ValueError(f"Each keyword dict must have '{field}'.")
        if int(kw['cpc_bid_micros']) <= 0:
            raise ValueError("cpc_bid_micros must be a positive integer.")

    if ctx:
        ctx.info(f"Updating bids for {len(keywords)} keyword(s) for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupCriteria:mutate"
        operations = [
            {
                "update": {
                    "resourceName": f"customers/{formatted_customer_id}/adGroupCriteria/{kw['ad_group_id'].strip()}~{kw['criterion_id'].strip()}",
                    "cpcBidMicros": str(int(kw['cpc_bid_micros']))
                },
                "updateMask": "cpcBidMicros"
            }
            for kw in keywords
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error updating keyword bids: {response.status_code} {response.reason} - {response.text}")

        updated = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully updated {len(updated)} keyword bid(s).")

        return {
            "keywords_updated": len(updated),
            "updated_resource_names": updated,
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def set_keyword_status(
    customer_id: str,
    ad_group_id: str,
    criterion_ids: List[str],
    status: str,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Pause, enable, or remove keywords in an ad group.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        ad_group_id: The ad group containing the keywords
        criterion_ids: List of keyword criterion IDs to update
            (get via run_gaql: SELECT ad_group_criterion.criterion_id FROM ad_group_criterion)
        status: New status - 'ENABLED', 'PAUSED', or 'REMOVED'
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of keywords updated
    """
    status = status.upper()
    if status not in ('ENABLED', 'PAUSED', 'REMOVED'):
        raise ValueError(f"Invalid status '{status}'. Must be ENABLED, PAUSED, or REMOVED.")
    if not criterion_ids:
        raise ValueError("criterion_ids must not be empty.")

    if ctx:
        ctx.info(f"Setting {len(criterion_ids)} keyword(s) to {status} in ad group {ad_group_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupCriteria:mutate"

        if status == 'REMOVED':
            operations = [
                {"remove": f"customers/{formatted_customer_id}/adGroupCriteria/{ad_group_id.strip()}~{cid.strip()}"}
                for cid in criterion_ids
            ]
        else:
            operations = [
                {
                    "update": {
                        "resourceName": f"customers/{formatted_customer_id}/adGroupCriteria/{ad_group_id.strip()}~{cid.strip()}",
                        "status": status
                    },
                    "updateMask": "status"
                }
                for cid in criterion_ids
            ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error updating keyword status: {response.status_code} {response.reason} - {response.text}")

        updated = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully set {len(updated)} keyword(s) to {status}.")

        return {
            "keywords_updated": len(updated),
            "status_set": status,
            "ad_group_id": ad_group_id,
            "customer_id": formatted_customer_id,
            "updated_resource_names": updated
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def create_campaign(
    customer_id: str,
    name: str,
    daily_budget_micros: int,
    advertising_channel_type: str = "SEARCH",
    bidding_strategy: str = "MANUAL_CPC",
    target_cpa_micros: Optional[int] = None,
    target_roas: Optional[float] = None,
    start_paused: bool = True,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Create a new campaign with a new daily budget.

    Creates the budget first, then the campaign. Campaigns start PAUSED by default for safety.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        name: Campaign name
        daily_budget_micros: Daily budget in micros (1,000,000 micros = $1.00).
            Example: 50000000 = $50/day
        advertising_channel_type: Campaign type. Options: SEARCH, DISPLAY, VIDEO,
            SHOPPING, PERFORMANCE_MAX (default: SEARCH)
        bidding_strategy: Bidding strategy. Options: MANUAL_CPC, TARGET_CPA,
            TARGET_ROAS, MAXIMIZE_CONVERSIONS, MAXIMIZE_CONVERSION_VALUE
            (default: MANUAL_CPC)
        target_cpa_micros: Target CPA in micros (required if bidding_strategy=TARGET_CPA)
        target_roas: Target ROAS as a decimal, e.g. 3.0 = 300% ROAS
            (required if bidding_strategy=TARGET_ROAS)
        start_paused: Create campaign in PAUSED status (default True for safety)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of the created budget and campaign
    """
    valid_channel_types = {'SEARCH', 'DISPLAY', 'VIDEO', 'SHOPPING', 'PERFORMANCE_MAX'}
    valid_bidding = {'MANUAL_CPC', 'TARGET_CPA', 'TARGET_ROAS', 'MAXIMIZE_CONVERSIONS', 'MAXIMIZE_CONVERSION_VALUE'}
    advertising_channel_type = advertising_channel_type.upper()
    bidding_strategy = bidding_strategy.upper()

    if advertising_channel_type not in valid_channel_types:
        raise ValueError(f"Invalid advertising_channel_type. Must be one of: {', '.join(sorted(valid_channel_types))}")
    if bidding_strategy not in valid_bidding:
        raise ValueError(f"Invalid bidding_strategy. Must be one of: {', '.join(sorted(valid_bidding))}")
    if bidding_strategy == 'TARGET_CPA' and not target_cpa_micros:
        raise ValueError("target_cpa_micros is required when bidding_strategy=TARGET_CPA")
    if bidding_strategy == 'TARGET_ROAS' and not target_roas:
        raise ValueError("target_roas is required when bidding_strategy=TARGET_ROAS")
    if daily_budget_micros <= 0:
        raise ValueError("daily_budget_micros must be positive.")

    if ctx:
        ctx.info(f"Creating campaign '{name}' for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        if ctx:
            ctx.info(f"Creating budget (${round(daily_budget_micros / 1_000_000, 2)}/day)...")

        budget_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignBudgets:mutate"
        budget_response = _make_request(requests.post, budget_url, headers, json_body={
            "operations": [{
                "create": {
                    "name": f"{name} Budget",
                    "amountMicros": str(daily_budget_micros),
                    "deliveryMethod": "STANDARD"
                }
            }]
        })

        if not budget_response.ok:
            raise Exception(f"Error creating budget: {budget_response.status_code} {budget_response.reason} - {budget_response.text}")

        budget_resource = budget_response.json().get('results', [{}])[0].get('resourceName', '')
        if not budget_resource:
            raise Exception("Budget created but resource name was not returned.")

        if ctx:
            ctx.info(f"Budget created: {budget_resource}. Creating campaign...")

        campaign_create = {
            "name": name,
            "status": "PAUSED" if start_paused else "ENABLED",
            "advertisingChannelType": advertising_channel_type,
            "campaignBudget": budget_resource,
            "networkSettings": {
                "targetGoogleSearch": True,
                "targetSearchNetwork": True,
                "targetContentNetwork": False,
                "targetPartnerSearchNetwork": False
            }
        }

        if bidding_strategy == 'MANUAL_CPC':
            campaign_create['manualCpc'] = {"enhancedCpcEnabled": False}
        elif bidding_strategy == 'TARGET_CPA':
            campaign_create['targetCpa'] = {"targetCpaMicros": str(target_cpa_micros)}
        elif bidding_strategy == 'TARGET_ROAS':
            campaign_create['targetRoas'] = {"targetRoas": target_roas}
        elif bidding_strategy == 'MAXIMIZE_CONVERSIONS':
            campaign_create['maximizeConversions'] = {}
        elif bidding_strategy == 'MAXIMIZE_CONVERSION_VALUE':
            campaign_create['maximizeConversionValue'] = {}

        campaign_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaigns:mutate"
        campaign_response = _make_request(requests.post, campaign_url, headers, json_body={"operations": [{"create": campaign_create}]})

        if not campaign_response.ok:
            raise Exception(f"Error creating campaign: {campaign_response.status_code} {campaign_response.reason} - {campaign_response.text}")

        campaign_resource = campaign_response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Campaign created: {campaign_resource}")

        return {
            "campaign_created": campaign_resource,
            "budget_created": budget_resource,
            "name": name,
            "status": "PAUSED" if start_paused else "ENABLED",
            "advertising_channel_type": advertising_channel_type,
            "bidding_strategy": bidding_strategy,
            "daily_budget_micros": daily_budget_micros,
            "daily_budget_dollars": round(daily_budget_micros / 1_000_000, 2),
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def create_ad_group(
    customer_id: str,
    campaign_id: str,
    name: str,
    cpc_bid_micros: int = 1000000,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Create a new ad group in a campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to create the ad group in
        name: Ad group name
        cpc_bid_micros: Default CPC bid in micros (default 1000000 = $1.00)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource name of the created ad group
    """
    if cpc_bid_micros <= 0:
        raise ValueError("cpc_bid_micros must be a positive integer.")

    if ctx:
        ctx.info(f"Creating ad group '{name}' in campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroups:mutate"
        response = _make_request(requests.post, url, headers, json_body={
            "operations": [{
                "create": {
                    "name": name,
                    "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                    "status": "ENABLED",
                    "cpcBidMicros": str(cpc_bid_micros)
                }
            }]
        })

        if not response.ok:
            raise Exception(f"Error creating ad group: {response.status_code} {response.reason} - {response.text}")

        resource_name = response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Ad group created: {resource_name}")

        return {
            "ad_group_created": resource_name,
            "name": name,
            "campaign_id": campaign_id,
            "cpc_bid_micros": cpc_bid_micros,
            "cpc_bid_dollars": round(cpc_bid_micros / 1_000_000, 2),
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def set_ad_status(
    customer_id: str,
    ads: List[Dict[str, str]],
    status: str,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Pause or enable one or more ads.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        ads: List of ad dicts. Each must have 'ad_group_id' and 'ad_id'.
            (get ad_id via run_gaql: SELECT ad_group_ad.ad.id FROM ad_group_ad)
            Example: [{"ad_group_id": "123456", "ad_id": "789012"}]
        status: New status - 'ENABLED' or 'PAUSED'
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of ads updated
    """
    status = status.upper()
    if status not in ('ENABLED', 'PAUSED'):
        raise ValueError(f"Invalid status '{status}'. Must be ENABLED or PAUSED.")
    if not ads:
        raise ValueError("ads list must not be empty.")
    for ad in ads:
        if 'ad_group_id' not in ad or 'ad_id' not in ad:
            raise ValueError("Each ad dict must have 'ad_group_id' and 'ad_id'.")

    if ctx:
        ctx.info(f"Setting {len(ads)} ad(s) to {status} for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupAds:mutate"
        operations = [
            {
                "update": {
                    "resourceName": f"customers/{formatted_customer_id}/adGroupAds/{ad['ad_group_id'].strip()}~{ad['ad_id'].strip()}",
                    "status": status
                },
                "updateMask": "status"
            }
            for ad in ads
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error updating ad status: {response.status_code} {response.reason} - {response.text}")

        updated = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully set {len(updated)} ad(s) to {status}.")

        return {
            "ads_updated": len(updated),
            "status_set": status,
            "updated_resource_names": updated,
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_sitelinks(
    customer_id: str,
    campaign_id: str,
    sitelinks: List[Dict[str, str]],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add sitelink assets to a campaign.

    Creates each sitelink as an asset then links it to the campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to add sitelinks to
        sitelinks: List of sitelink dicts. Each must have:
            - 'link_text': Anchor text shown in the ad (max 25 chars)
            - 'final_url': Landing page URL
            Optional:
            - 'description1': First description line (max 35 chars)
            - 'description2': Second description line (max 35 chars)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of created assets and campaign asset links
    """
    if not sitelinks:
        raise ValueError("sitelinks list must not be empty.")
    for sl in sitelinks:
        if 'link_text' not in sl or 'final_url' not in sl:
            raise ValueError("Each sitelink must have 'link_text' and 'final_url'.")
        if len(sl['link_text']) > 25:
            raise ValueError(f"link_text too long (max 25 chars): '{sl['link_text']}'")
        if sl.get('description1') and len(sl['description1']) > 35:
            raise ValueError(f"description1 too long (max 35 chars): '{sl['description1']}'")
        if sl.get('description2') and len(sl['description2']) > 35:
            raise ValueError(f"description2 too long (max 35 chars): '{sl['description2']}'")

    if ctx:
        ctx.info(f"Adding {len(sitelinks)} sitelink(s) to campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/assets:mutate"
        asset_operations = []
        for sl in sitelinks:
            sitelink_asset = {"linkText": sl['link_text']}
            if sl.get('description1'):
                sitelink_asset['description1'] = sl['description1']
            if sl.get('description2'):
                sitelink_asset['description2'] = sl['description2']
            asset_operations.append({
                "create": {
                    "name": f"Sitelink: {sl['link_text']}",
                    "finalUrls": [sl['final_url']],
                    "sitelinkAsset": sitelink_asset,
                }
            })

        asset_response = _make_request(requests.post, asset_url, headers, json_body={"operations": asset_operations})
        if not asset_response.ok:
            raise Exception(f"Error creating sitelink assets: {asset_response.status_code} {asset_response.reason} - {asset_response.text}")

        asset_rns = [r.get('resourceName', '') for r in asset_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Created {len(asset_rns)} asset(s). Linking to campaign...")

        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignAssets:mutate"
        link_operations = [
            {
                "create": {
                    "asset": rn,
                    "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                    "fieldType": "SITELINK"
                }
            }
            for rn in asset_rns
        ]

        link_response = _make_request(requests.post, link_url, headers, json_body={"operations": link_operations})
        if not link_response.ok:
            raise Exception(f"Error linking sitelinks to campaign: {link_response.status_code} {link_response.reason} - {link_response.text}")

        link_rns = [r.get('resourceName', '') for r in link_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(link_rns)} sitelink(s) to campaign.")

        return {
            "sitelinks_added": len(link_rns),
            "campaign_id": campaign_id,
            "asset_resource_names": asset_rns,
            "campaign_asset_resource_names": link_rns,
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_callouts(
    customer_id: str,
    campaign_id: str,
    callout_texts: List[str],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add callout assets to a campaign.

    Creates each callout as an asset then links it to the campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to add callouts to
        callout_texts: List of callout strings, each max 25 characters.
            Example: ["Free Shipping", "24/7 Support", "No Hidden Fees"]
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of created assets and campaign asset links
    """
    if not callout_texts:
        raise ValueError("callout_texts must not be empty.")
    for text in callout_texts:
        if len(text) > 25:
            raise ValueError(f"Callout text too long (max 25 chars): '{text}' ({len(text)} chars)")

    if ctx:
        ctx.info(f"Adding {len(callout_texts)} callout(s) to campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/assets:mutate"
        asset_response = _make_request(requests.post, asset_url, headers, json_body={
            "operations": [
                {"create": {"name": f"Callout: {text}", "calloutAsset": {"calloutText": text}}}
                for text in callout_texts
            ]
        })

        if not asset_response.ok:
            raise Exception(f"Error creating callout assets: {asset_response.status_code} {asset_response.reason} - {asset_response.text}")

        asset_rns = [r.get('resourceName', '') for r in asset_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Created {len(asset_rns)} callout asset(s). Linking to campaign...")

        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignAssets:mutate"
        link_response = _make_request(requests.post, link_url, headers, json_body={
            "operations": [
                {
                    "create": {
                        "asset": rn,
                        "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                        "fieldType": "CALLOUT"
                    }
                }
                for rn in asset_rns
            ]
        })

        if not link_response.ok:
            raise Exception(f"Error linking callouts to campaign: {link_response.status_code} {link_response.reason} - {link_response.text}")

        link_rns = [r.get('resourceName', '') for r in link_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(link_rns)} callout(s) to campaign.")

        return {
            "callouts_added": len(link_rns),
            "campaign_id": campaign_id,
            "asset_resource_names": asset_rns,
            "campaign_asset_resource_names": link_rns,
            "customer_id": formatted_customer_id
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def set_bid_adjustment(
    customer_id: str,
    campaign_id: str,
    adjustment_type: str,
    bid_modifier: float,
    device_type: str = "",
    geo_target_id: int = 0,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Set a bid adjustment (modifier) for a device type or location on a campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to set the adjustment on
        adjustment_type: Type of adjustment - 'DEVICE' or 'LOCATION'
        bid_modifier: The bid modifier as a multiplier.
            1.0 = no change, 1.2 = +20%, 0.8 = -20%, 0.0 = exclude (device only)
        device_type: Required when adjustment_type='DEVICE'.
            Options: 'MOBILE', 'TABLET', 'DESKTOP'
        geo_target_id: Required when adjustment_type='LOCATION'.
            Google Ads geo target constant ID (e.g. 2840=US, 1014221=New York, 1006094=London)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource name of the created or updated campaign criterion
    """
    adjustment_type = adjustment_type.upper()
    if adjustment_type not in ('DEVICE', 'LOCATION'):
        raise ValueError("adjustment_type must be 'DEVICE' or 'LOCATION'.")
    if bid_modifier < 0.0 or bid_modifier > 10.0:
        raise ValueError("bid_modifier must be between 0.0 and 10.0.")
    if adjustment_type == 'DEVICE':
        device_type = device_type.upper()
        if device_type not in ('MOBILE', 'TABLET', 'DESKTOP'):
            raise ValueError("device_type must be MOBILE, TABLET, or DESKTOP when adjustment_type=DEVICE.")
    if adjustment_type == 'LOCATION' and not geo_target_id:
        raise ValueError("geo_target_id is required when adjustment_type=LOCATION.")

    if ctx:
        ctx.info(f"Setting {adjustment_type} bid adjustment ({bid_modifier}x) on campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"

        if adjustment_type == 'DEVICE':
            query = (
                f"SELECT campaign_criterion.criterion_id, campaign_criterion.device.type "
                f"FROM campaign_criterion "
                f"WHERE campaign.id = {campaign_id.strip()} "
                f"AND campaign_criterion.type = 'DEVICE' "
                f"AND campaign_criterion.device.type = '{device_type}'"
            )
            result = execute_gaql(formatted_customer_id, query, mgr)
            rows = result.get('results', [])

            headers = get_headers_with_auto_token()
            if manager_id:
                headers['login-customer-id'] = mgr

            if rows:
                criterion_id = rows[0].get('campaignCriterion', {}).get('criterionId', '')
                operation = {
                    "update": {
                        "resourceName": f"customers/{formatted_customer_id}/campaignCriteria/{campaign_id.strip()}~{criterion_id}",
                        "bidModifier": bid_modifier
                    },
                    "updateMask": "bidModifier"
                }
            else:
                operation = {
                    "create": {
                        "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                        "device": {"type": device_type},
                        "bidModifier": bid_modifier
                    }
                }
        else:  # LOCATION
            headers = get_headers_with_auto_token()
            if manager_id:
                headers['login-customer-id'] = mgr

            operation = {
                "create": {
                    "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                    "location": {"geoTargetConstant": f"geoTargetConstants/{geo_target_id}"},
                    "bidModifier": bid_modifier
                }
            }

        response = _make_request(requests.post, url, headers, json_body={"operations": [operation]})

        if not response.ok:
            raise Exception(f"Error setting bid adjustment: {response.status_code} {response.reason} - {response.text}")

        resource_name = response.json().get('results', [{}])[0].get('resourceName', '')
        pct = round((bid_modifier - 1) * 100, 1)

        if ctx:
            ctx.info(f"Bid adjustment set: {resource_name} ({pct:+.1f}%)")

        result = {
            "adjustment_set": resource_name,
            "adjustment_type": adjustment_type,
            "bid_modifier": bid_modifier,
            "bid_modifier_pct": f"{pct:+.1f}%",
            "campaign_id": campaign_id,
            "customer_id": formatted_customer_id
        }
        if adjustment_type == 'DEVICE':
            result['device_type'] = device_type
        else:
            result['geo_target_id'] = geo_target_id

        return result

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def update_bidding_strategy(
    customer_id: str,
    campaign_id: str,
    bidding_strategy: str,
    target_cpa_micros: Optional[int] = None,
    target_roas: Optional[float] = None,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Change the bidding strategy on an existing campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to update
        bidding_strategy: New strategy. Options: MANUAL_CPC, TARGET_CPA,
            TARGET_ROAS, MAXIMIZE_CONVERSIONS, MAXIMIZE_CONVERSION_VALUE
        target_cpa_micros: Target CPA in micros (required if bidding_strategy=TARGET_CPA).
            Example: 5000000 = $5.00 CPA
        target_roas: Target ROAS as decimal (required if bidding_strategy=TARGET_ROAS).
            Example: 3.0 = 300% ROAS
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Updated campaign resource name and new bidding strategy
    """
    valid = {'MANUAL_CPC', 'TARGET_CPA', 'TARGET_ROAS', 'MAXIMIZE_CONVERSIONS', 'MAXIMIZE_CONVERSION_VALUE'}
    bidding_strategy = bidding_strategy.upper()
    if bidding_strategy not in valid:
        raise ValueError(f"Invalid bidding_strategy. Must be one of: {', '.join(sorted(valid))}")
    if bidding_strategy == 'TARGET_CPA' and not target_cpa_micros:
        raise ValueError("target_cpa_micros is required when bidding_strategy=TARGET_CPA")
    if bidding_strategy == 'TARGET_ROAS' and not target_roas:
        raise ValueError("target_roas is required when bidding_strategy=TARGET_ROAS")

    if ctx:
        ctx.info(f"Updating bidding strategy for campaign {campaign_id} to {bidding_strategy}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        resource_name = f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}"
        update_body = {"resourceName": resource_name}

        if bidding_strategy == 'MANUAL_CPC':
            update_body['manualCpc'] = {"enhancedCpcEnabled": False}
            update_mask = "manualCpc"
        elif bidding_strategy == 'TARGET_CPA':
            update_body['targetCpa'] = {"targetCpaMicros": str(target_cpa_micros)}
            update_mask = "targetCpa"
        elif bidding_strategy == 'TARGET_ROAS':
            update_body['targetRoas'] = {"targetRoas": target_roas}
            update_mask = "targetRoas"
        elif bidding_strategy == 'MAXIMIZE_CONVERSIONS':
            update_body['maximizeConversions'] = {}
            update_mask = "maximizeConversions"
        elif bidding_strategy == 'MAXIMIZE_CONVERSION_VALUE':
            update_body['maximizeConversionValue'] = {}
            update_mask = "maximizeConversionValue"

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaigns:mutate"
        response = _make_request(requests.post, url, headers, json_body={
            "operations": [{"update": update_body, "updateMask": update_mask}]
        })

        if not response.ok:
            raise Exception(f"Error updating bidding strategy: {response.status_code} {response.reason} - {response.text}")

        updated_rn = response.json().get('results', [{}])[0].get('resourceName', resource_name)

        if ctx:
            ctx.info(f"Bidding strategy updated to {bidding_strategy}.")

        result = {
            "campaign_updated": updated_rn,
            "campaign_id": campaign_id,
            "bidding_strategy": bidding_strategy,
            "customer_id": formatted_customer_id,
        }
        if target_cpa_micros:
            result['target_cpa_micros'] = target_cpa_micros
            result['target_cpa_dollars'] = round(target_cpa_micros / 1_000_000, 2)
        if target_roas:
            result['target_roas'] = target_roas

        return result

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_location_targeting(
    customer_id: str,
    campaign_id: str,
    geo_target_ids: List[int],
    negative: bool = False,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add location targets (inclusions or exclusions) to a campaign.

    This adds actual geo targeting (not bid adjustments). Use set_bid_adjustment
    for bid modifiers on existing location targets.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to add location targeting to
        geo_target_ids: List of Google Ads geo target constant IDs.
            Common values: 2840=US, 2826=UK, 2124=Canada, 2036=Australia,
            2276=Germany, 2250=France, 2356=India, 2392=Japan
            Find others via: run_gaql on geo_target_constant resource
        negative: True to exclude these locations, False to target them (default False)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of location criteria added
    """
    if not geo_target_ids:
        raise ValueError("geo_target_ids must not be empty.")

    action = "Excluding" if negative else "Targeting"
    if ctx:
        ctx.info(f"{action} {len(geo_target_ids)} location(s) for campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"
        operations = [
            {
                "create": {
                    "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                    "negative": negative,
                    "location": {"geoTargetConstant": f"geoTargetConstants/{gid}"}
                }
            }
            for gid in geo_target_ids
        ]

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error adding location targeting: {response.status_code} {response.reason} - {response.text}")

        created = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(created)} location target(s).")

        return {
            "locations_added": len(created),
            "negative": negative,
            "geo_target_ids": geo_target_ids,
            "campaign_id": campaign_id,
            "created_resource_names": created,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def set_ad_schedule(
    customer_id: str,
    campaign_id: str,
    schedules: List[Dict[str, Any]],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Set ad schedule (dayparting) for a campaign.

    Each slot defines a day + hour range with an optional bid modifier.
    Creating any schedule means ads only show during scheduled slots.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to set the ad schedule on
        schedules: List of schedule slot dicts. Each must have:
            - 'day': Day of week. Options: MONDAY, TUESDAY, WEDNESDAY, THURSDAY,
              FRIDAY, SATURDAY, SUNDAY
            - 'start_hour': Start hour 0-23 (integer)
            - 'end_hour': End hour 1-24 (integer, use 24 for midnight)
            Optional:
            - 'bid_modifier': Bid multiplier for this slot (default 1.0 = no change)
            - 'start_minute': ZERO, FIFTEEN, THIRTY, FORTY_FIVE (default ZERO)
            - 'end_minute': ZERO, FIFTEEN, THIRTY, FORTY_FIVE (default ZERO)
            Example: [{"day": "MONDAY", "start_hour": 9, "end_hour": 17, "bid_modifier": 1.2}]
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Summary of schedule slots created
    """
    valid_days = {'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'}

    if not schedules:
        raise ValueError("schedules list must not be empty.")

    for s in schedules:
        if 'day' not in s or 'start_hour' not in s or 'end_hour' not in s:
            raise ValueError("Each schedule must have 'day', 'start_hour', and 'end_hour'.")
        if s['day'].upper() not in valid_days:
            raise ValueError(f"Invalid day '{s['day']}'. Must be one of: {', '.join(sorted(valid_days))}")
        if not (0 <= int(s['start_hour']) <= 23):
            raise ValueError("start_hour must be 0-23.")
        if not (1 <= int(s['end_hour']) <= 24):
            raise ValueError("end_hour must be 1-24.")

    if ctx:
        ctx.info(f"Setting {len(schedules)} ad schedule slot(s) for campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"
        operations = []
        for s in schedules:
            slot = {
                "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                "adSchedule": {
                    "dayOfWeek": s['day'].upper(),
                    "startHour": int(s['start_hour']),
                    "startMinute": s.get('start_minute', 'ZERO').upper(),
                    "endHour": int(s['end_hour']),
                    "endMinute": s.get('end_minute', 'ZERO').upper(),
                }
            }
            if 'bid_modifier' in s:
                slot['bidModifier'] = float(s['bid_modifier'])
            operations.append({"create": slot})

        response = _make_request(requests.post, url, headers, json_body={"operations": operations})

        if not response.ok:
            raise Exception(f"Error setting ad schedule: {response.status_code} {response.reason} - {response.text}")

        created = [r.get('resourceName', '') for r in response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully created {len(created)} ad schedule slot(s).")

        return {
            "slots_created": len(created),
            "campaign_id": campaign_id,
            "created_resource_names": created,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_demographic_adjustment(
    customer_id: str,
    campaign_id: str,
    demographic_type: str,
    value: str,
    bid_modifier: float,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Set a bid adjustment for a demographic segment on a campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to set the adjustment on
        demographic_type: Type of demographic - 'AGE' or 'GENDER'
        value: The demographic value.
            For AGE: AGE_RANGE_18_24, AGE_RANGE_25_34, AGE_RANGE_35_44,
              AGE_RANGE_45_54, AGE_RANGE_55_64, AGE_RANGE_65_UP, AGE_RANGE_UNDETERMINED
            For GENDER: MALE, FEMALE, UNDETERMINED
        bid_modifier: Bid multiplier (e.g. 1.2 = +20%, 0.8 = -20%, 0.0 = exclude)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource name of the created or updated campaign criterion
    """
    demographic_type = demographic_type.upper()
    if demographic_type not in ('AGE', 'GENDER'):
        raise ValueError("demographic_type must be 'AGE' or 'GENDER'.")

    valid_age = {
        'AGE_RANGE_18_24', 'AGE_RANGE_25_34', 'AGE_RANGE_35_44',
        'AGE_RANGE_45_54', 'AGE_RANGE_55_64', 'AGE_RANGE_65_UP', 'AGE_RANGE_UNDETERMINED'
    }
    valid_gender = {'MALE', 'FEMALE', 'UNDETERMINED'}
    value = value.upper()

    if demographic_type == 'AGE' and value not in valid_age:
        raise ValueError(f"Invalid age value '{value}'. Must be one of: {', '.join(sorted(valid_age))}")
    if demographic_type == 'GENDER' and value not in valid_gender:
        raise ValueError(f"Invalid gender value '{value}'. Must be one of: {', '.join(sorted(valid_gender))}")
    if bid_modifier < 0.0 or bid_modifier > 10.0:
        raise ValueError("bid_modifier must be between 0.0 and 10.0.")

    if ctx:
        ctx.info(f"Setting {demographic_type} ({value}) bid adjustment ({bid_modifier}x) on campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        criterion_type_filter = "'AGE_RANGE'" if demographic_type == 'AGE' else "'GENDER'"
        query = (
            f"SELECT campaign_criterion.criterion_id, campaign_criterion.type "
            f"FROM campaign_criterion "
            f"WHERE campaign.id = {campaign_id.strip()} "
            f"AND campaign_criterion.type = {criterion_type_filter}"
        )

        if demographic_type == 'AGE':
            query += f" AND campaign_criterion.age_range.type = '{value}'"
        else:
            query += f" AND campaign_criterion.gender.type = '{value}'"

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        headers = get_headers_with_auto_token()
        if manager_id:
            headers['login-customer-id'] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"

        if rows:
            criterion_id = rows[0].get('campaignCriterion', {}).get('criterionId', '')
            operation = {
                "update": {
                    "resourceName": f"customers/{formatted_customer_id}/campaignCriteria/{campaign_id.strip()}~{criterion_id}",
                    "bidModifier": bid_modifier
                },
                "updateMask": "bidModifier"
            }
        else:
            criterion_body = {
                "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                "bidModifier": bid_modifier
            }
            if demographic_type == 'AGE':
                criterion_body['ageRange'] = {"type": value}
            else:
                criterion_body['gender'] = {"type": value}
            operation = {"create": criterion_body}

        response = _make_request(requests.post, url, headers, json_body={"operations": [operation]})

        if not response.ok:
            raise Exception(f"Error setting demographic adjustment: {response.status_code} {response.reason} - {response.text}")

        resource_name = response.json().get('results', [{}])[0].get('resourceName', '')
        pct = round((bid_modifier - 1) * 100, 1)

        if ctx:
            ctx.info(f"Demographic adjustment set: {resource_name} ({pct:+.1f}%)")

        return {
            "adjustment_set": resource_name,
            "demographic_type": demographic_type,
            "value": value,
            "bid_modifier": bid_modifier,
            "bid_modifier_pct": f"{pct:+.1f}%",
            "campaign_id": campaign_id,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_structured_snippets(
    customer_id: str,
    campaign_id: str,
    snippets: List[Dict[str, Any]],
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add structured snippet assets to a campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to add structured snippets to
        snippets: List of snippet dicts. Each must have:
            - 'header': The snippet header. Common values: Amenities, Brands,
              Courses, Destinations, Featured hotels, Insurance coverage, Models,
              Neighborhoods, Service catalog, Shows, Styles, Types
            - 'values': List of 3-10 value strings (each max 25 chars)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of created assets and campaign asset links
    """
    if not snippets:
        raise ValueError("snippets list must not be empty.")
    for s in snippets:
        if 'header' not in s or 'values' not in s:
            raise ValueError("Each snippet must have 'header' and 'values'.")
        if len(s['values']) < 3 or len(s['values']) > 10:
            raise ValueError(f"Snippet values must have 3-10 items, got {len(s['values'])}.")
        for v in s['values']:
            if len(v) > 25:
                raise ValueError(f"Snippet value too long (max 25 chars): '{v}'")

    if ctx:
        ctx.info(f"Adding {len(snippets)} structured snippet(s) to campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/assets:mutate"
        asset_response = _make_request(requests.post, asset_url, headers, json_body={
            "operations": [
                {
                    "create": {
                        "name": f"Snippet: {s['header']}",
                        "structuredSnippetAsset": {
                            "header": s['header'],
                            "values": s['values']
                        }
                    }
                }
                for s in snippets
            ]
        })

        if not asset_response.ok:
            raise Exception(f"Error creating snippet assets: {asset_response.status_code} {asset_response.reason} - {asset_response.text}")

        asset_rns = [r.get('resourceName', '') for r in asset_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Created {len(asset_rns)} snippet asset(s). Linking to campaign...")

        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignAssets:mutate"
        link_response = _make_request(requests.post, link_url, headers, json_body={
            "operations": [
                {
                    "create": {
                        "asset": rn,
                        "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                        "fieldType": "STRUCTURED_SNIPPET"
                    }
                }
                for rn in asset_rns
            ]
        })

        if not link_response.ok:
            raise Exception(f"Error linking snippets to campaign: {link_response.status_code} {link_response.reason} - {link_response.text}")

        link_rns = [r.get('resourceName', '') for r in link_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Successfully added {len(link_rns)} structured snippet(s).")

        return {
            "snippets_added": len(link_rns),
            "campaign_id": campaign_id,
            "asset_resource_names": asset_rns,
            "campaign_asset_resource_names": link_rns,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_call_asset(
    customer_id: str,
    campaign_id: str,
    phone_number: str,
    country_code: str = "US",
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add a phone number call asset to a campaign.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        campaign_id: The campaign ID to add the call asset to
        phone_number: The phone number to display (e.g. "+1-555-123-4567")
        country_code: ISO 3166-1 alpha-2 country code (default "US")
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of the created asset and campaign asset link
    """
    if not phone_number:
        raise ValueError("phone_number must not be empty.")

    if ctx:
        ctx.info(f"Adding call asset ({phone_number}) to campaign {campaign_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        asset_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/assets:mutate"
        asset_response = _make_request(requests.post, asset_url, headers, json_body={
            "operations": [{
                "create": {
                    "name": f"Call: {phone_number}",
                    "callAsset": {
                        "phoneNumber": phone_number,
                        "countryCode": country_code.upper()
                    }
                }
            }]
        })

        if not asset_response.ok:
            raise Exception(f"Error creating call asset: {asset_response.status_code} {asset_response.reason} - {asset_response.text}")

        asset_rn = asset_response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Call asset created. Linking to campaign...")

        link_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignAssets:mutate"
        link_response = _make_request(requests.post, link_url, headers, json_body={
            "operations": [{
                "create": {
                    "asset": asset_rn,
                    "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                    "fieldType": "CALL"
                }
            }]
        })

        if not link_response.ok:
            raise Exception(f"Error linking call asset to campaign: {link_response.status_code} {link_response.reason} - {link_response.text}")

        link_rn = link_response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Call asset linked: {link_rn}")

        return {
            "call_asset_added": True,
            "phone_number": phone_number,
            "country_code": country_code.upper(),
            "campaign_id": campaign_id,
            "asset_resource_name": asset_rn,
            "campaign_asset_resource_name": link_rn,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def add_audience_targeting(
    customer_id: str,
    user_list_id: str,
    campaign_id: str = "",
    ad_group_id: str = "",
    bid_modifier: float = 1.0,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Add a remarketing/user list audience to a campaign or ad group.

    Adds the audience in observation mode (bid-only), meaning ads still show
    to everyone but you can adjust bids for this audience.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        user_list_id: The user list (remarketing list) ID to target.
            Find user list IDs via: run_gaql with 'SELECT user_list.id, user_list.name FROM user_list'
        campaign_id: Campaign ID for campaign-level audience (provide this OR ad_group_id)
        ad_group_id: Ad group ID for ad-group-level audience (provide this OR campaign_id)
        bid_modifier: Optional bid modifier for this audience (default 1.0 = no adjustment)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource name of the created audience criterion
    """
    if not campaign_id and not ad_group_id:
        raise ValueError("Provide either campaign_id or ad_group_id.")
    if campaign_id and ad_group_id:
        raise ValueError("Provide either campaign_id or ad_group_id, not both.")
    if bid_modifier < 0.0 or bid_modifier > 10.0:
        raise ValueError("bid_modifier must be between 0.0 and 10.0.")

    level = "campaign" if campaign_id else "ad group"
    if ctx:
        ctx.info(f"Adding user list {user_list_id} to {level} for customer {customer_id}...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        user_list_rn = f"customers/{formatted_customer_id}/userLists/{user_list_id.strip()}"

        if campaign_id:
            url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignCriteria:mutate"
            criterion = {
                "campaign": f"customers/{formatted_customer_id}/campaigns/{campaign_id.strip()}",
                "userList": {"userList": user_list_rn},
            }
        else:
            url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/adGroupCriteria:mutate"
            criterion = {
                "adGroup": f"customers/{formatted_customer_id}/adGroups/{ad_group_id.strip()}",
                "userList": {"userList": user_list_rn},
            }

        if bid_modifier != 1.0:
            criterion['bidModifier'] = bid_modifier

        response = _make_request(requests.post, url, headers, json_body={"operations": [{"create": criterion}]})

        if not response.ok:
            raise Exception(f"Error adding audience: {response.status_code} {response.reason} - {response.text}")

        resource_name = response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Audience targeting added: {resource_name}")

        return {
            "audience_added": resource_name,
            "user_list_id": user_list_id,
            "level": level,
            "campaign_id": campaign_id or None,
            "ad_group_id": ad_group_id or None,
            "bid_modifier": bid_modifier,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def create_shared_negative_list(
    customer_id: str,
    list_name: str,
    keywords: List[Dict[str, str]],
    campaign_ids: List[str] = None,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Create a shared negative keyword list and optionally apply it to campaigns.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        list_name: Name for the shared negative keyword list
        keywords: List of keyword dicts. Each must have 'text' and 'match_type'.
            match_type options: 'BROAD', 'PHRASE', 'EXACT'
            Example: [{"text": "free", "match_type": "BROAD"}]
        campaign_ids: Optional list of campaign IDs to apply the list to immediately
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        Resource names of the shared set, keywords added, and campaign links
    """
    if not keywords:
        raise ValueError("keywords list must not be empty.")

    valid_match_types = {'BROAD', 'PHRASE', 'EXACT'}
    for kw in keywords:
        if 'text' not in kw or 'match_type' not in kw:
            raise ValueError("Each keyword must have 'text' and 'match_type'.")
        if kw['match_type'].upper() not in valid_match_types:
            raise ValueError(f"Invalid match_type '{kw['match_type']}'. Must be BROAD, PHRASE, or EXACT.")

    if ctx:
        ctx.info(f"Creating shared negative list '{list_name}' with {len(keywords)} keyword(s)...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        formatted_customer_id = format_customer_id(customer_id)

        if manager_id:
            headers['login-customer-id'] = format_customer_id(manager_id)

        ss_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/sharedSets:mutate"
        ss_response = _make_request(requests.post, ss_url, headers, json_body={
            "operations": [{"create": {"name": list_name, "type": "NEGATIVE_KEYWORDS"}}]
        })

        if not ss_response.ok:
            raise Exception(f"Error creating shared set: {ss_response.status_code} {ss_response.reason} - {ss_response.text}")

        shared_set_rn = ss_response.json().get('results', [{}])[0].get('resourceName', '')

        if ctx:
            ctx.info(f"Shared set created: {shared_set_rn}. Adding keywords...")

        ssc_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/sharedSetCriteria:mutate"
        ssc_response = _make_request(requests.post, ssc_url, headers, json_body={
            "operations": [
                {
                    "create": {
                        "sharedSet": shared_set_rn,
                        "keyword": {"text": kw['text'], "matchType": kw['match_type'].upper()}
                    }
                }
                for kw in keywords
            ]
        })

        if not ssc_response.ok:
            raise Exception(f"Error adding keywords to shared set: {ssc_response.status_code} {ssc_response.reason} - {ssc_response.text}")

        keyword_rns = [r.get('resourceName', '') for r in ssc_response.json().get('results', [])]

        campaign_link_rns = []
        if campaign_ids:
            if ctx:
                ctx.info(f"Linking shared set to {len(campaign_ids)} campaign(s)...")

            css_url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/campaignSharedSets:mutate"
            css_response = _make_request(requests.post, css_url, headers, json_body={
                "operations": [
                    {
                        "create": {
                            "campaign": f"customers/{formatted_customer_id}/campaigns/{cid.strip()}",
                            "sharedSet": shared_set_rn
                        }
                    }
                    for cid in campaign_ids
                ]
            })

            if not css_response.ok:
                raise Exception(f"Error linking shared set to campaigns: {css_response.status_code} {css_response.reason} - {css_response.text}")

            campaign_link_rns = [r.get('resourceName', '') for r in css_response.json().get('results', [])]

        if ctx:
            ctx.info(f"Shared negative list created with {len(keyword_rns)} keyword(s) and linked to {len(campaign_link_rns)} campaign(s).")

        return {
            "shared_set_created": shared_set_rn,
            "list_name": list_name,
            "keywords_added": len(keyword_rns),
            "campaigns_linked": len(campaign_link_rns),
            "keyword_resource_names": keyword_rns,
            "campaign_link_resource_names": campaign_link_rns,
            "customer_id": formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise


@mcp.tool
def get_recommendations(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None
) -> Dict[str, Any]:
    """Fetch Google Ads automated recommendations for the account.

    Returns active, non-dismissed recommendations grouped by type.
    Use run_gaql or the Google Ads UI to apply recommendations.

    Args:
        customer_id: The Google Ads customer ID (10 digits, no dashes)
        manager_id: Manager ID if the account is accessed through an MCC

    Returns:
        List of recommendations grouped by type with campaign context
    """
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching recommendations for customer {customer_id}...")

    try:
        formatted_customer_id = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = """
            SELECT
                recommendation.resource_name,
                recommendation.type,
                recommendation.dismissed,
                recommendation.campaign,
                recommendation.ad_group,
                recommendation.impact.base_metrics.impressions,
                recommendation.impact.potential_metrics.impressions,
                recommendation.impact.base_metrics.clicks,
                recommendation.impact.potential_metrics.clicks,
                recommendation.impact.base_metrics.cost_micros,
                recommendation.impact.potential_metrics.cost_micros,
                recommendation.impact.base_metrics.conversions,
                recommendation.impact.potential_metrics.conversions
            FROM recommendation
            WHERE recommendation.dismissed = FALSE
        """

        result = execute_gaql(formatted_customer_id, query, mgr)
        rows = result.get('results', [])

        if ctx:
            ctx.info(f"Found {len(rows)} active recommendation(s).")

        by_type: Dict[str, list] = {}
        for row in rows:
            rec = row.get('recommendation', {})
            rtype = rec.get('type', 'UNKNOWN')
            impact = rec.get('impact', {})
            base = impact.get('baseMetrics', {})
            potential = impact.get('potentialMetrics', {})

            entry = {
                'resource_name': rec.get('resourceName', ''),
                'campaign': rec.get('campaign', ''),
                'ad_group': rec.get('adGroup', ''),
                'impact': {
                    'base_impressions': int(base.get('impressions', 0)),
                    'potential_impressions': int(potential.get('impressions', 0)),
                    'base_clicks': int(base.get('clicks', 0)),
                    'potential_clicks': int(potential.get('clicks', 0)),
                    'base_conversions': float(base.get('conversions', 0)),
                    'potential_conversions': float(potential.get('conversions', 0)),
                }
            }

            if rtype not in by_type:
                by_type[rtype] = []
            by_type[rtype].append(entry)

        return {
            'recommendations_by_type': by_type,
            'total_recommendations': len(rows),
            'types_found': sorted(by_type.keys()),
            'customer_id': formatted_customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"An unexpected error occurred: {e}")
        raise
