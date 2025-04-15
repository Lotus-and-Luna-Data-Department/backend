# flows/shopify_flows/returns/validate_returns.py

import logging
import ast
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def validate_shopify_returns(
    refunds_csv: str = "output/refunds.csv",
    refund_line_items_csv: str = "output/refund_line_items.csv"
):
    """
    Reads the newly created refunds CSVs, logs summary metrics.
    We only do top-line returns here.
    """
    logger.info(f"validate_shopify_returns: reading {refunds_csv} and {refund_line_items_csv}")
    refunds_df = pd.read_csv(refunds_csv) if refunds_csv else pd.DataFrame()
    line_items_df = pd.read_csv(refund_line_items_csv) if refund_line_items_csv else pd.DataFrame()

    if refunds_df.empty:
        logger.warning("validate_shopify_returns: No refunds found!")
        print("No refunds found. Skipping validation.")
        return

    # Convert numeric columns if needed
    numeric_cols = [
        'total_refunded', 'gross_returns', 'discounts_returned',
        'shipping_returned', 'taxes_returned', 'return_fees',
        'net_returns', 'total_returns'
    ]
    for col in numeric_cols:
        if col in refunds_df.columns:
            refunds_df[col] = pd.to_numeric(refunds_df[col], errors='coerce').fillna(0)

    total_gross_returns = refunds_df['gross_returns'].sum() if 'gross_returns' in refunds_df.columns else 0.0
    total_refunded = refunds_df['total_refunded'].sum() if 'total_refunded' in refunds_df.columns else 0.0
    total_taxes = refunds_df['taxes_returned'].sum() if 'taxes_returned' in refunds_df.columns else 0.0
    total_shipping = refunds_df['shipping_returned'].sum() if 'shipping_returned' in refunds_df.columns else 0.0

    logger.info("=== Validation Stats (Shopify Returns) ===")
    logger.info(f"Total refunded (from total_refunded): ${total_refunded:,.2f}")
    logger.info(f"Gross returns (refund_subtotal):       ${total_gross_returns:,.2f}")
    logger.info(f"Shipping returned:                     ${total_shipping:,.2f}")
    logger.info(f"Taxes returned:                        ${total_taxes:,.2f}")

    print("=== Validation Stats (Shopify Returns) ===")
    print(f"Total refunded: ${total_refunded:,.2f}")
    print(f"Gross returns:  ${total_gross_returns:,.2f}")
    print(f"Shipping:       ${total_shipping:,.2f}")
    print(f"Taxes:          ${total_taxes:,.2f}")

    # Additional info
    print("--- Additional Info ---")
    print(f"Refunds count: {len(refunds_df)}")
    print(f"Refund line items count: {len(line_items_df)}")
