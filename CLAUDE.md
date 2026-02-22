# Google Ads MCP Server

## What this is
A FastMCP server exposing 92 Google Ads tools to Claude Desktop via the MCP protocol.
Connects to the Google Ads REST API v23 directly (no client library).

## How to run / test
```bash
# Verify all tools load (should show 92 tools)
.venv/bin/python -c "
import server
tools = server.mcp._tool_manager._tools
print(f'Total tools: {len(tools)}')
"

# Run with HTTP transport for debugging
.venv/bin/python server.py --http
# then hit http://127.0.0.1:8000/mcp

# Claude Desktop uses STDIO (default)
.venv/bin/python server.py
```

## Architecture

### Entry point
`server.py` — thin entrypoint: loads .env, imports all tool modules (triggering @mcp.tool registration), registers the gaql:// resource, runs FastMCP.

### Shared instance
`mcp_instance.py` — holds the single `FastMCP("Google Ads Tools")` instance imported by all tool modules.

### Tool modules (`tools/`)
| File | Tools | Description |
|------|-------|-------------|
| `accounts.py` | 1 | list_accounts (with nested MCC sub-accounts) |
| `read.py` | 9 | run_gaql, account performance, quality scores, disapproved ads, auction insights, anomalies, search terms, campaign details, budget pacing |
| `write.py` | 23 | All mutations: keywords, ads, campaigns, budgets, extensions, bidding, targeting, recommendations |
| `reporting.py` | 12 | keyword/ad/ad-group/geo/device/dayparting/landing page perf, impression share, wasted spend, asset perf, PMax report, shopping perf |
| `conversions.py` | 4 | list/create/update conversion actions, conversion performance |
| `labels.py` | 4 | list/create labels, apply/remove to campaigns/ad groups/ads/keywords |
| `account.py` | 4 | account info, update settings, billing info, list accessible accounts |
| `utils.py` | 3 | change history, ad preview, policy violations |
| `audiences.py` | 5 | user lists, customer match list, topic/placement targeting, audience segments |
| `assets.py` | 5 | list assets, create image asset, price/promotion/image extensions |
| `campaigns.py` | 7 | list campaigns/ad groups/keywords/ads, update ad group, end date, network settings |
| `ads.py` | 6 | update RSA, ad strength, create display/call-only ads, apply/dismiss recommendations |
| `bids.py` | 4 | move keywords, list/create shared budgets, apply shared budget |
| `pmax.py` | 3 | create PMax campaign/asset group, list asset groups |
| `shopping.py` | 2 | create shopping campaign, list product groups |

### Auth (`oauth/google_auth.py`)
- OAuth 2.0 via `google-auth-oauthlib`, token stored in `google_ads_token.json`
- Key exports: `format_customer_id`, `get_headers_with_auto_token`, `execute_gaql`, `_make_request`, `API_VERSION`, `GOOGLE_ADS_DEVELOPER_TOKEN`
- `_make_request(method, url, headers, json_body)` — retries on 429/500/503 with exponential backoff
- `execute_gaql(customer_id, query, manager_id)` — auto-paginates via nextPageToken

## Adding a new tool
1. Pick the right module (or create a new one in `tools/`)
2. Add `@mcp.tool` decorator — FastMCP auto-generates the schema from type hints and docstring
3. If new file: `import tools.newmodule  # noqa: F401, E402` in `server.py`
4. Verify: `.venv/bin/python -c "import server; print(len(server.mcp._tool_manager._tools))"`

## Common patterns

### Read tool (GAQL query)
```python
@mcp.tool
def my_tool(customer_id: str, ..., manager_id: str = "", ctx: Context = None) -> Dict[str, Any]:
    cid = format_customer_id(customer_id)
    mgr = format_customer_id(manager_id) if manager_id else ""
    result = execute_gaql(cid, "SELECT ... FROM ... WHERE ...", mgr)
    rows = result.get("results", [])
    ...
```

### Write tool (REST mutation)
```python
@mcp.tool
def my_tool(customer_id: str, ..., manager_id: str = "", ctx: Context = None) -> Dict[str, Any]:
    cid = format_customer_id(customer_id)
    mgr = format_customer_id(manager_id) if manager_id else ""
    headers = get_headers_with_auto_token(cid, mgr)
    if mgr:
        headers["login-customer-id"] = mgr
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/RESOURCE:mutate"
    body = {"operations": [{"create": {...}}]}
    resp = _make_request(requests.post, url, headers, body)
    if not resp.ok:
        raise Exception(f"API error: {resp.status_code} {resp.text}")
    ...
```

## Known fixes / past bugs
- **add_sitelinks**: `finalUrls` must be on the outer `Asset` object, NOT inside `sitelinkAsset`. Fixed in commit 9314aeb.

## Claude Desktop config
```json
{
  "mcpServers": {
    "google-ads": {
      "command": "/Users/joaobarbosa/google-ads-mcp-server/.venv/bin/python",
      "args": ["/Users/joaobarbosa/google-ads-mcp-server/server.py"]
    }
  }
}
```
Located at: `~/Library/Application Support/Claude/claude_desktop_config.json`

## Environment variables (`.env`)
- `GOOGLE_ADS_DEVELOPER_TOKEN` — required
- `GOOGLE_ADS_CLIENT_ID` — OAuth client ID
- `GOOGLE_ADS_CLIENT_SECRET` — OAuth client secret
- Token file: `google_ads_token.json` (auto-refreshed)
