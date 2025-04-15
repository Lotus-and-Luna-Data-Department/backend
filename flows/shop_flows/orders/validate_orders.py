# flows/shopify_flows/orders/validate_orders.py

import logging
import ast
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_list_for_validation(x):
    """Parse a stringified list or float for shipping_charges in validation stats."""
    if pd.isna(x) or x is None:
        logger.debug(f"parse_list_for_validation: shipping_charges is None/NaN => 0.0")
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            arr = ast.literal_eval(x)
            if not isinstance(arr, list):
                logger.debug(f"Non-list shipping_charges: {arr}")
                return 0.0
            return sum(float(i) for i in arr if isinstance(i, (int, float, str)))
        except (ValueError, SyntaxError) as e:
            logger.error(f"parse_list_for_validation: error parsing '{x}', {e}")
            return 0.0
    elif isinstance(x, list):
        return sum(float(i) for i in x if isinstance(i, (int, float, str)))
    else:
        logger.warning(f"parse_list_for_validation: unexpected type {type(x)} => 0.0")
        return 0.0

@task
def validate_shopify_orders(
    orders_csv: str = "output/orders.csv",
    line_items_csv: str = "output/order_line_items.csv"
):
    """
    Reads the newly created CSV files, logs summary metrics,
    for Shopify Orders only (no refunds).
    """
    logger.info("validate_shopify_orders: Reading %s and %s", orders_csv, line_items_csv)
    orders_df = pd.read_csv(orders_csv)
    line_items_df = pd.read_csv(line_items_csv)

    # Basic column checks
    for col in ['current_total_price', 'current_discounts', 'total_tax']:
        if col not in orders_df.columns:
            orders_df[col] = 0.0

    orders_df['current_total_price'] = orders_df['current_total_price'].astype(float)
    orders_df['current_discounts'] = orders_df['current_discounts'].astype(float)
    orders_df['total_tax'] = orders_df['total_tax'].astype(float)

    # shipping_sum
    orders_df['shipping_sum'] = orders_df['shipping_charges'].apply(parse_list_for_validation)

    # shipping_charge_taxes_sum
    def parse_shipping_charge_taxes_sum(x):
        if pd.notna(x) and isinstance(x, str) and x.replace('.', '').replace('-', '').isdigit():
            return float(x)
        return 0.0
    if 'shipping_charge_taxes' not in orders_df.columns:
        orders_df['shipping_charge_taxes'] = ""
    orders_df['shipping_charge_taxes_sum'] = orders_df['shipping_charge_taxes'].apply(parse_shipping_charge_taxes_sum)

    # Aggregations
    total_gross = orders_df['original_total_price'].sum()
    total_discounts = orders_df['current_discounts'].sum()
    total_tax = orders_df['total_tax'].sum()
    total_shipping = orders_df['shipping_sum'].sum()

    # no refunds in this flow => total_returns=0
    total_returns = 0.0
    net_sales = total_gross - total_discounts - total_returns
    total_sales = net_sales + total_shipping + total_tax

    logger.info("=== Validation Stats (Shopify Orders) ===")
    logger.info(f"Gross sales:       ${total_gross:,.2f}")
    logger.info(f"Discounts:         -${total_discounts:,.2f}")
    logger.info(f"Returns:           -${total_returns:,.2f}")
    logger.info(f"Net sales:         ${net_sales:,.2f}")
    logger.info(f"Shipping charges:  ${total_shipping:,.2f}")
    logger.info(f"Taxes:             ${total_tax:,.2f}")
    logger.info(f"Total sales:       ${total_sales:,.2f}")

    logger.info("--- Additional Info ---")
    logger.info(f"Orders count: {len(orders_df)}")
    logger.info(f"Line Items count: {len(line_items_df)}")

    # Print to console as well
    print("=== Validation Stats (Shopify Orders) ===")
    print(f"Gross sales:       ${total_gross:,.2f}")
    print(f"Discounts:         -${total_discounts:,.2f}")
    print(f"Net sales:         ${net_sales:,.2f}")
    print(f"Shipping charges:  ${total_shipping:,.2f}")
    print(f"Taxes:             ${total_tax:,.2f}")
    print(f"Total sales:       ${total_sales:,.2f}")
    print("--- Additional Info ---")
    print(f"Orders count: {len(orders_df)}")
    print(f"Line Items count: {len(line_items_df)}")
