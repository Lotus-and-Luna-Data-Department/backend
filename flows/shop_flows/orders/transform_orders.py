import logging
import pandas as pd
import ast
import numpy as np
from typing import List, Dict, Tuple
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def process_shipping_charge_taxes(x):
    """
    Parse and sum shipping_charge_taxes field properly.
    Kept from your original logic, used below by preprocess_orders().
    """
    if isinstance(x, (pd.Series, np.ndarray)):
        x = x.tolist()

    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None

    if isinstance(x, str):
        try:
            parsed = ast.literal_eval(x.strip())
            x = parsed
        except (ValueError, SyntaxError):
            try:
                return float(x)  # Cast to float directly
            except ValueError:
                logger.warning(f"Error parsing shipping_charge_taxes from '{x}'")
                return None

    if isinstance(x, list):
        valid_values = []
        for item in x:
            try:
                valid_values.append(float(item))
            except (ValueError, TypeError):
                pass
        return sum(valid_values) if valid_values else 0.0

    if isinstance(x, (int, float)):
        return float(x)

    logger.warning(f"Unexpected type for shipping_charge_taxes: {type(x)}, value: {x}")
    return None

def preprocess_orders(df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess Shopify orders DataFrame to match your final extracted fields.
    Now correctly casts numeric fields to float for database compatibility.
    """
    logger.info("preprocess_orders: Preprocessing orders DataFrame, shape=%s", df_orders.shape)

    expected_columns = [
        'order_id',
        'created_at',
        'updated_at',
        'display_financial_status',
        'original_total_price',
        'current_discounts',
        'current_subtotal',
        'current_total_price',
        'channel_name',
        'sub_channel_name',
        'total_tax',
        'shipping_charges',
        'shipping_charge_taxes',
        'carrier_identifier',
        # New fields
        'shipping_address1',
        'shipping_city',
        'shipping_zip',
        'shipping_country_code',
        'discount_codes',
        'discount_total_amount',
    ]

    if df_orders.empty:
        logger.warning("preprocess_orders: Empty DataFrame. Returning empty with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    df_orders = df_orders.copy()

    # Stringify relevant columns
    string_columns = [
        'order_id',
        'created_at',
        'updated_at',
        'display_financial_status',
        'channel_name',
        'sub_channel_name',
        'carrier_identifier',
        # New string fields
        'shipping_address1',
        'shipping_city',
        'shipping_zip',
        'shipping_country_code',
        'discount_codes',
    ]
    for col in string_columns:
        if col in df_orders.columns:
            df_orders[col] = df_orders[col].astype(str).where(df_orders[col].notna(), None)

    # Parse shipping_charge_taxes properly
    if 'shipping_charge_taxes' in df_orders.columns:
        df_orders['shipping_charge_taxes'] = df_orders['shipping_charge_taxes'].apply(process_shipping_charge_taxes)

    # Cast numeric fields as float (correct schema compliance)
    numeric_fields = [
        'original_total_price',
        'current_discounts',
        'current_subtotal',
        'current_total_price',
        'total_tax',
        'shipping_charges',
        'shipping_charge_taxes',
        # New numeric field
        'discount_total_amount',
    ]
    for col in numeric_fields:
        if col in df_orders.columns:
            df_orders[col] = pd.to_numeric(df_orders[col], errors='coerce')

    # Parse timestamps
    timestamp_columns = ['created_at', 'updated_at']
    for col in timestamp_columns:
        if col in df_orders.columns:
            df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')
            df_orders[col] = df_orders[col].where(df_orders[col].notna(), None)

    logger.debug("preprocess_orders: Final shape %s", df_orders.shape)
    return df_orders

def preprocess_order_line_items(df_line_items: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess Shopify order line items DataFrame, preserving all item-level fields.
    Now properly casts quantity and price fields to numeric types.
    """
    logger.info("preprocess_order_line_items: Preprocessing line items, shape=%s", df_line_items.shape)

    expected_columns = [
        'order_id',
        'line_item_id',
        'created_at',
        'updated_at',
        'title',
        'quantity',
        'sku',
        'product_id',
        'product_type',
        'variant_id',
        'variant_sku',
        'variant_price',
        'original_total',
        'discounted_total',
        # Updated fields: replaced discount_allocations with discount_codes and discount_amounts
        'discount_codes',
        'discount_amounts',
    ]

    if df_line_items.empty:
        logger.warning("preprocess_order_line_items: Empty DataFrame. Returning empty with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    df_line_items = df_line_items.copy()

    # Ensure certain columns are treated as strings
    string_columns = [
        'order_id', 'line_item_id', 'title', 'sku', 'product_id',
        'product_type', 'variant_id', 'variant_sku',
        # Updated string fields: replaced discount_allocations with discount_codes and discount_amounts
        'discount_codes',
        'discount_amounts',
    ]
    for col in string_columns:
        if col in df_line_items.columns:
            df_line_items[col] = df_line_items[col].astype(str).where(df_line_items[col].notna(), None)

    # Cast numeric fields properly (int/float as appropriate)
    numeric_fields = ['quantity', 'variant_price', 'original_total', 'discounted_total']
    for col in numeric_fields:
        if col in df_line_items.columns:
            if col == 'quantity':
                df_line_items[col] = pd.to_numeric(df_line_items[col], errors='coerce').astype('Int64')
            else:
                df_line_items[col] = pd.to_numeric(df_line_items[col], errors='coerce')

    # Parse timestamps
    timestamp_columns = ['created_at', 'updated_at']
    for col in timestamp_columns:
        if col in df_line_items.columns:
            df_line_items[col] = pd.to_datetime(df_line_items[col], errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')
            df_line_items[col] = df_line_items[col].where(df_line_items[col].notna(), None)

    logger.debug("preprocess_order_line_items: Final shape %s", df_line_items.shape)
    return df_line_items

@task
def transform_shopify_orders(
    orders_list: List[Dict],
    line_items_list: List[Dict]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert raw Shopify orders & line items (Python lists of dicts) into preprocessed DataFrames
    matching your final extraction schema.
    """
    logger.info("transform_shopify_orders: Converting raw lists to DataFrames.")
    orders_df = pd.DataFrame(orders_list) if orders_list else pd.DataFrame()
    line_items_df = pd.DataFrame(line_items_list) if line_items_list else pd.DataFrame()

    logger.debug(
        "transform_shopify_orders: Initial shapes => orders_df=%s, line_items_df=%s",
        orders_df.shape, line_items_df.shape
    )

    # Preprocess both DataFrames
    orders_df = preprocess_orders(orders_df)
    line_items_df = preprocess_order_line_items(line_items_df)

    logger.info(
        "transform_shopify_orders: After preprocessing => orders_df=%s, line_items_df=%s",
        orders_df.shape, line_items_df.shape
    )
    return orders_df, line_items_df