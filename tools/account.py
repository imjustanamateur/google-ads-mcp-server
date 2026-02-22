"""Account-level management tools for Google Ads MCP Server."""
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
def get_account_info(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get detailed account information including settings, currency, timezone, and auto-tagging."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching account info for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = """
            SELECT
                customer.id,
                customer.descriptive_name,
                customer.currency_code,
                customer.time_zone,
                customer.status,
                customer.manager,
                customer.test_account,
                customer.auto_tagging_enabled,
                customer.tracking_url_template,
                customer.final_url_suffix,
                customer.optimization_score
            FROM customer
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        if not rows:
            return {"error": "No account info found", "customer_id": customer_id}

        c = rows[0].get("customer", {})

        return {
            "id": str(c.get("id", "")),
            "name": c.get("descriptiveName", ""),
            "currency_code": c.get("currencyCode", ""),
            "time_zone": c.get("timeZone", ""),
            "status": c.get("status", ""),
            "is_manager": c.get("manager", False),
            "is_test_account": c.get("testAccount", False),
            "auto_tagging_enabled": c.get("autoTaggingEnabled", False),
            "tracking_url_template": c.get("trackingUrlTemplate", ""),
            "final_url_suffix": c.get("finalUrlSuffix", ""),
            "optimization_score": c.get("optimizationScore"),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_account_info failed: {str(e)}")
        raise


@mcp.tool
def update_account_settings(
    customer_id: str,
    auto_tagging_enabled: bool = None,
    tracking_url_template: str = "",
    final_url_suffix: str = "",
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Update account-level settings. Pass only the fields you want to change."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Updating account settings for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        headers = get_headers_with_auto_token(cid, mgr)
        if mgr:
            headers["login-customer-id"] = mgr

        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}:mutate"

        update_body: Dict[str, Any] = {
            "resourceName": f"customers/{cid}"
        }
        update_mask_fields = []

        if auto_tagging_enabled is not None:
            update_body["autoTaggingEnabled"] = auto_tagging_enabled
            update_mask_fields.append("auto_tagging_enabled")
        if tracking_url_template:
            update_body["trackingUrlTemplate"] = tracking_url_template
            update_mask_fields.append("tracking_url_template")
        if final_url_suffix:
            update_body["finalUrlSuffix"] = final_url_suffix
            update_mask_fields.append("final_url_suffix")

        if not update_mask_fields:
            return {"message": "No fields to update provided.", "customer_id": customer_id}

        body = {
            "customer": update_body,
            "updateMask": ",".join(update_mask_fields),
        }

        resp = _make_request(requests.post, url, headers, body)
        if not resp.ok:
            raise Exception(f"API error: {resp.status_code} {resp.text}")

        if ctx:
            ctx.info(f"Account settings updated: {update_mask_fields}")

        return {
            "updated_fields": update_mask_fields,
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"update_account_settings failed: {str(e)}")
        raise


@mcp.tool
def get_billing_info(
    customer_id: str,
    manager_id: str = "",
    ctx: Context = None,
) -> Dict[str, Any]:
    """Get billing setup and payment information for the account."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching billing info for customer {customer_id}...")

    try:
        cid = format_customer_id(customer_id)
        mgr = format_customer_id(manager_id) if manager_id else ""

        query = """
            SELECT
                billing_setup.id,
                billing_setup.status,
                billing_setup.payments_account,
                billing_setup.payments_account_info.payments_account_id,
                billing_setup.payments_account_info.payments_account_name,
                billing_setup.payments_account_info.payments_profile_id,
                billing_setup.payments_account_info.payments_profile_name,
                billing_setup.start_date_time
            FROM billing_setup
            WHERE billing_setup.status = 'APPROVED'
        """

        result = execute_gaql(cid, query, mgr)
        rows = result.get("results", [])

        billing_setups = []
        for row in rows:
            bs = row.get("billingSetup", {})
            pai = bs.get("paymentsAccountInfo", {})
            billing_setups.append({
                "id": str(bs.get("id", "")),
                "status": bs.get("status", ""),
                "payments_account": bs.get("paymentsAccount", ""),
                "payments_account_id": pai.get("paymentsAccountId", ""),
                "payments_account_name": pai.get("paymentsAccountName", ""),
                "payments_profile_id": pai.get("paymentsProfileId", ""),
                "payments_profile_name": pai.get("paymentsProfileName", ""),
                "start_date_time": bs.get("startDateTime", ""),
            })

        if ctx:
            ctx.info(f"Found {len(billing_setups)} billing setup(s).")

        return {
            "billing_setups": billing_setups,
            "total": len(billing_setups),
            "customer_id": customer_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"get_billing_info failed: {str(e)}")
        raise


@mcp.tool
def list_accessible_accounts(
    manager_id: str,
    ctx: Context = None,
) -> Dict[str, Any]:
    """List all accounts accessible under a manager (MCC) account, including nested sub-accounts."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("Google Ads Developer Token is not set in environment variables.")

    if ctx:
        ctx.info(f"Fetching accessible accounts under manager {manager_id}...")

    try:
        mid = format_customer_id(manager_id)

        query = """
            SELECT
                customer_client.id,
                customer_client.descriptive_name,
                customer_client.level,
                customer_client.manager,
                customer_client.status,
                customer_client.currency_code,
                customer_client.time_zone,
                customer_client.test_account
            FROM customer_client
            WHERE customer_client.status != 'CANCELED'
            ORDER BY customer_client.level ASC, customer_client.descriptive_name ASC
        """

        result = execute_gaql(mid, query, mid)
        rows = result.get("results", [])

        accounts = []
        for row in rows:
            cc = row.get("customerClient", {})
            accounts.append({
                "id": format_customer_id(str(cc.get("id", ""))),
                "name": cc.get("descriptiveName", ""),
                "level": cc.get("level", 0),
                "is_manager": cc.get("manager", False),
                "status": cc.get("status", ""),
                "currency_code": cc.get("currencyCode", ""),
                "time_zone": cc.get("timeZone", ""),
                "is_test_account": cc.get("testAccount", False),
            })

        if ctx:
            ctx.info(f"Found {len(accounts)} accessible accounts.")

        return {
            "accounts": accounts,
            "total": len(accounts),
            "manager_id": manager_id,
        }

    except Exception as e:
        if ctx:
            ctx.error(f"list_accessible_accounts failed: {str(e)}")
        raise
