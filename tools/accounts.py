import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List
from fastmcp import Context
from mcp_instance import mcp
from oauth.google_auth import (
    format_customer_id, get_headers_with_auto_token,
    execute_gaql, API_VERSION, GOOGLE_ADS_DEVELOPER_TOKEN,
    _make_request,
)

logger = logging.getLogger(__name__)


def _get_customer_info(cid: str):
    """Return (name, is_manager) for a customer ID."""
    try:
        result = execute_gaql(cid, "SELECT customer.descriptive_name, customer.manager FROM customer")
        rows = result.get('results', [])
        if not rows:
            return "Name not available", False
        c = rows[0].get('customer', {})
        return c.get('descriptiveName', 'Name not available'), bool(c.get('manager', False))
    except Exception:
        return "Name not available", False


def _get_sub_accounts(manager_id: str) -> List[Dict[str, Any]]:
    try:
        query = (
            "SELECT customer_client.id, customer_client.descriptive_name, "
            "customer_client.level, customer_client.manager "
            "FROM customer_client WHERE customer_client.level > 0"
        )
        result = execute_gaql(manager_id, query)
        subs = []
        for row in result.get('results', []):
            client = row.get('customerClient', {}) or row.get('customer_client', {})
            cid = format_customer_id(str(client.get('id', '')))
            subs.append({
                'id': cid,
                'name': client.get('descriptiveName', f"Sub-account {cid}"),
                'access_type': 'managed',
                'is_manager': bool(client.get('manager', False)),
                'parent_id': manager_id,
                'level': int(client.get('level', 0))
            })
        return subs
    except Exception:
        return []


@mcp.tool
def list_accounts(ctx: Context = None) -> Dict[str, Any]:
    """List all accessible accounts including nested sub-accounts."""
    if ctx:
        ctx.info("Checking credentials and preparing to list accounts...")

    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    try:
        headers = get_headers_with_auto_token()
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
        resp = _make_request(requests.get, url, headers)
        if not resp.ok:
            raise Exception(f"Error listing accounts: {resp.status_code} {resp.reason} - {resp.text}")

        resource_names = resp.json().get('resourceNames', [])
        if not resource_names:
            return {'accounts': [], 'message': 'No accessible accounts found.'}

        top_level_ids = [rn.split('/')[-1] for rn in resource_names]

        # Fetch top-level account info in parallel
        if ctx:
            ctx.info(f"Found {len(top_level_ids)} top-level accounts. Fetching details in parallel...")

        accounts = []
        seen = set()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_get_customer_info, format_customer_id(cid)): cid for cid in top_level_ids}
            for future in as_completed(futures):
                cid = futures[future]
                fid = format_customer_id(cid)
                name, is_manager = future.result()
                accounts.append({
                    'id': fid, 'name': name,
                    'access_type': 'direct', 'is_manager': is_manager, 'level': 0
                })
                seen.add(fid)

        # Fetch sub-accounts for managers (also in parallel)
        manager_ids = [a['id'] for a in accounts if a['is_manager']]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_get_sub_accounts, mid): mid for mid in manager_ids}
            for future in as_completed(futures):
                for sub in future.result():
                    if sub['id'] not in seen:
                        accounts.append(sub)
                        seen.add(sub['id'])

        if ctx:
            ctx.info(f"Found {len(accounts)} total accounts.")

        return {'accounts': accounts, 'total_accounts': len(accounts)}

    except Exception as e:
        if ctx:
            ctx.error(f"Error listing accounts: {e}")
        raise
