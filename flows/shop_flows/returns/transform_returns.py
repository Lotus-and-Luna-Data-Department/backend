# flows/shopify_flows/returns/transform_returns.py

import logging
import pandas as pd
from typing import List, Dict, Tuple
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def preprocess_refunds(df_refunds: pd.DataFrame) -> pd.DataFrame:
    """Preprocess refunds DataFrame to match schema, handling empty case."""
    logger.info("Preprocessing refunds DataFrame")

    expected_columns = [
        'refund_id', 'order_id', 'created_at', 'processed_at', 'updated_at', 'total_refunded',
        'refund_amount', 'sale_id', 'product_title', 'gross_returns', 'discounts_returned',
        'shipping_returned', 'taxes_returned', 'return_fees', 'net_returns', 'total_returns'
    ]
    if df_refunds.empty:
        logger.warning("preprocess_refunds: Empty DataFrame received; returning empty DataFrame with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    df_refunds = df_refunds.copy()

    # Ensure numeric fields are present and properly typed
    numeric_columns = [
        'total_refunded', 'gross_returns', 'discounts_returned',
        'shipping_returned', 'taxes_returned', 'return_fees',
        'net_returns', 'total_returns'
    ]
    for col in numeric_columns:
        if col not in df_refunds.columns:
            logger.debug(f"Column '{col}' not found, creating as None.")
            df_refunds[col] = None
        df_refunds[col] = pd.to_numeric(df_refunds[col], errors='coerce')
        df_refunds[col] = df_refunds[col].where(df_refunds[col].notna(), None)

    # Validate and format timestamp columns
    timestamp_columns = ['created_at', 'processed_at', 'updated_at']
    for col in timestamp_columns:
        if col not in df_refunds.columns:
            logger.debug(f"Timestamp column '{col}' not found, creating as None.")
            df_refunds[col] = None
        df_refunds[col] = pd.to_datetime(df_refunds[col], errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')
        df_refunds[col] = df_refunds[col].where(df_refunds[col].notna(), None)

    return df_refunds


def ensure_refund_line_item_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure refund_line_item_id is present and deterministic.
    If not provided, generate as 'refund_id_line_item_id'.
    """
    if df.empty:
        logger.warning("ensure_refund_line_item_id: Empty DataFrame received; skipping generation.")
        return df

    if 'refund_line_item_id' in df.columns and df['refund_line_item_id'].notna().all():
        logger.info("ensure_refund_line_item_id: Column already provided and complete. Skipping generation.")
        return df

    logger.info("ensure_refund_line_item_id: Generating refund_line_item_id as 'refund_id_line_item_id'.")
    df['refund_line_item_id'] = df.apply(
        lambda row: f"{row['refund_id']}_{row['line_item_id']}" if pd.notna(row['refund_id']) and pd.notna(row['line_item_id']) else None,
        axis=1
    )
    return df


def preprocess_refund_line_items(df_refund_line_items: pd.DataFrame) -> pd.DataFrame:
    """Preprocess refund line items DataFrame to match schema, handling empty case."""
    logger.info("Preprocessing refund line items DataFrame")

    expected_columns = [
        'refund_id', 'order_id', 'quantity', 'subtotal', 'tax_subtotal', 'line_item_id',
        'title', 'sku', 'product_id', 'product_type', 'variant_id', 'variant_sku', 'refund_line_item_id'
    ]
    if df_refund_line_items.empty:
        logger.warning("preprocess_refund_line_items: Empty DataFrame received; returning empty DataFrame with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    df_refund_line_items = df_refund_line_items.copy()

    # Ensure numeric fields are typed properly
    numeric_columns = ['quantity', 'subtotal', 'tax_subtotal']
    for col in numeric_columns:
        df_refund_line_items[col] = pd.to_numeric(df_refund_line_items[col], errors='coerce')
        df_refund_line_items[col] = df_refund_line_items[col].where(df_refund_line_items[col].notna(), None)

    # Ensure quantity is an integer type that can handle NaNs
    df_refund_line_items['quantity'] = df_refund_line_items['quantity'].round().astype('Int64')

    # Ensure deterministic refund_line_item_id
    df_refund_line_items = ensure_refund_line_item_id(df_refund_line_items)

    return df_refund_line_items


@task
def transform_shopify_returns(
    refunds_list: List[Dict],
    refund_line_items_list: List[Dict]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert raw refunds & refund line items into DataFrames,
    ensuring empty cases return properly structured DataFrames.
    """
    logger.info("transform_shopify_returns: Converting raw lists into DataFrames.")
    refunds_df = pd.DataFrame(refunds_list) if refunds_list else pd.DataFrame()
    refund_line_items_df = pd.DataFrame(refund_line_items_list) if refund_line_items_list else pd.DataFrame()

    logger.debug(
        f"transform_shopify_returns: Initial refunds_df shape={refunds_df.shape}, "
        f"refund_line_items_df shape={refund_line_items_df.shape}"
    )

    # Always preprocess — preprocessing will handle empty DataFrames and schema
    refunds_df = preprocess_refunds(refunds_df)
    refund_line_items_df = preprocess_refund_line_items(refund_line_items_df)

    logger.info("transform_shopify_returns: Done preprocessing.")
    logger.debug(
        f"transform_shopify_returns: Final refunds_df shape={refunds_df.shape}, "
        f"refund_line_items_df shape={refund_line_items_df.shape}"
    )
    return refunds_df, refund_line_items_df
