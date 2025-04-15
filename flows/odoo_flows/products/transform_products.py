import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw Odoo product variants DataFrame to aligned schema.
    Returns structured empty DataFrame with schema if input is empty.

    Final fields:
      - product_id
      - internal_reference
      - name
      - qty_on_hand
      - free_qty
      - outgoing_qty
      - incoming_qty
      - last_updated (renamed from write_date)
    """
    logger.info("=== transform_odoo_products: starting transform ===")

    expected_columns = [
        'product_id', 'internal_reference', 'name', 'qty_on_hand',
        'free_qty', 'outgoing_qty', 'incoming_qty', 'last_updated'
    ]

    if df.empty:
        logger.warning("No Odoo products found; returning empty DataFrame with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    # --- Map raw fields to standardized schema ---
    df_transformed = pd.DataFrame({
        'product_id': df['id'],
        'internal_reference': df['default_code'],
        'name': df['name'],
        'qty_on_hand': df['qty_available'],
        'free_qty': df['free_qty'],
        'outgoing_qty': df['outgoing_qty'],
        'incoming_qty': df['incoming_qty'],
        'last_updated': df['write_date']  # Renamed from write_date to last_updated
    })

    # --- Parse timestamps and handle NaT ---
    timestamp_columns = ['last_updated']
    for col in timestamp_columns:
        df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # --- Cast numeric fields ---
    numeric_columns = ['qty_on_hand', 'free_qty', 'outgoing_qty', 'incoming_qty']
    for col in numeric_columns:
        df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')

    # --- Ensure string columns are proper strings (null-safe) ---
    string_columns = ['product_id', 'internal_reference', 'name']
    for col in string_columns:
        df_transformed[col] = df_transformed[col].astype(str).where(df_transformed[col].notna(), None)

    logger.info(f"transform_odoo_products: After transformations, shape={df_transformed.shape}")
    return df_transformed