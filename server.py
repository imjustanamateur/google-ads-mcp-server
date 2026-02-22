"""Google Ads MCP Server - modular entry point."""

import logging
import sys

from dotenv import load_dotenv
load_dotenv()

from mcp_instance import mcp  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('google_ads_server')

logger.info("Starting Google Ads MCP Server...")

# Import tool modules so their @mcp.tool decorators register on the shared instance
import tools.accounts  # noqa: F401, E402
import tools.read      # noqa: F401, E402
import tools.write     # noqa: F401, E402


@mcp.resource("gaql://reference")
def gaql_reference() -> str:
    """Google Ads Query Language (GAQL) reference documentation."""
    return """Schema Format:
                ## Basic Query Structure
                '''
                SELECT field1, field2, ...
                FROM resource_type
                WHERE condition
                ORDER BY field [ASC|DESC]
                LIMIT n
                '''

                ## Common Field Types

                ### Resource Fields
                - campaign.id, campaign.name, campaign.status
                - ad_group.id, ad_group.name, ad_group.status
                - ad_group_ad.ad.id, ad_group_ad.ad.final_urls
                - ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type (for keyword_view)

                ### Metric Fields
                - metrics.impressions
                - metrics.clicks
                - metrics.cost_micros
                - metrics.conversions
                - metrics.conversions_value (direct conversion revenue - primary revenue metric)
                - metrics.ctr
                - metrics.average_cpc

                ### Segment Fields
                - segments.date
                - segments.device
                - segments.day_of_week

                ## Common WHERE Clauses

                ### Date Ranges
                - WHERE segments.date DURING LAST_7_DAYS
                - WHERE segments.date DURING LAST_30_DAYS
                - WHERE segments.date BETWEEN '2023-01-01' AND '2023-01-31'

                ### Filtering
                - WHERE campaign.status = 'ENABLED'
                - WHERE metrics.clicks > 100
                - WHERE campaign.name LIKE '%Brand%'
                - Use LIKE '%keyword%' instead of CONTAINS 'keyword' (CONTAINS not supported)

                EXAMPLE QUERIES:

                1. Basic campaign metrics:
                SELECT
                campaign.id,
                campaign.name,
                metrics.clicks,
                metrics.impressions,
                metrics.cost_micros
                FROM campaign
                WHERE segments.date DURING LAST_7_DAYS

                2. Ad group performance:
                SELECT
                campaign.id,
                ad_group.name,
                metrics.conversions,
                metrics.cost_micros,
                campaign.name
                FROM ad_group
                WHERE metrics.clicks > 100

                3. Keyword analysis (CORRECT field names):
                SELECT
                campaign.id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                metrics.average_position,
                metrics.ctr
                FROM keyword_view
                WHERE segments.date DURING LAST_30_DAYS
                ORDER BY metrics.impressions DESC

                4. Get conversion data with revenue:
                SELECT
                campaign.id,
                campaign.name,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions_value,
                metrics.cost_micros
                FROM campaign
                WHERE segments.date DURING LAST_30_DAYS

                IMPORTANT NOTES & COMMON ERRORS TO AVOID:

                ### Field Errors to Avoid:
                WRONG: campaign.campaign_budget.amount_micros
                CORRECT: campaign_budget.amount_micros (query from campaign_budget resource)

                WRONG: keyword.text, keyword.match_type
                CORRECT: ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type

                ### Required Fields:
                - Always include campaign.id when querying ad_group, keyword_view, or other campaign-related resources
                - Some resources require specific reference fields in SELECT clause

                ### Revenue Metrics:
                - metrics.conversions_value = Direct conversion revenue (use for ROI calculations)
                - metrics.all_conversions_value = Total attributed revenue (includes view-through)

                ### String Matching:
                - Use LIKE '%keyword%' not CONTAINS 'keyword'
                - GAQL does not support CONTAINS operator

                NOTE:
                - Date ranges must be finite: LAST_7_DAYS, LAST_30_DAYS, or BETWEEN dates
                - Cannot use open-ended ranges like >= '2023-01-31'
                - Always include campaign.id when error messages request it."""


if __name__ == "__main__":
    if "--http" in sys.argv:
        logger.info("Starting with HTTP transport on http://127.0.0.1:8000/mcp")
        mcp.run(transport="streamable-http", host="127.0.0.1", port=8000, path="/mcp")
    else:
        logger.info("Starting with STDIO transport for Claude Desktop")
        mcp.run(transport="stdio")
