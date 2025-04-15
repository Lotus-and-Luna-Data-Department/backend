import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_order_lines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw Odoo sale order lines DataFrame to aligned schema.
    Returns structured empty DataFrame with schema if input is empty.

    Final fields (for unified_order_lines and odoo_feature_order_lines):
      - line_item_id    (unified: from id)
      - order_id       (unified: from order_reference)
      - sales_date     (unified: from sales_date)
      - last_updated   (unified: renamed from write_date)
      - sku            (unified: from sku)
      - quantity       (unified: from quantity)
      - unit_price     (unified: from unit_price)
      - product_category (feature: from product_category)
      - to_invoice     (feature: derived from subtotal)
      - cost           (feature: from unit_cost)
      - sales_team     (feature: from sales_team)
    """
    logger.info("=== transform_odoo_order_lines: starting transform ===")

    expected_columns = [
        'line_item_id', 'order_id', 'sales_date', 'last_updated', 'sku',
        'quantity', 'unit_price', 'product_category', 'to_invoice', 'cost', 'sales_team'
    ]

    if df.empty:
        logger.warning("No sale order lines found; returning empty DataFrame with expected schema.")
        return pd.DataFrame(columns=expected_columns)

    # --- Map raw fields to standardized schema ---
    df_transformed = pd.DataFrame({
        'line_item_id': df['id'],                    # Unified: Order line ID
        'order_id': df['order_reference'],           # Unified: Parent order name (e.g., SO1234)
        'sales_date': df['sales_date'],              # Unified: Added from API
        'last_updated': df['write_date'],            # Unified: Last update timestamp (renamed from write_date)
        'sku': df['sku'],                            # Unified: Custom SKU field
        'quantity': df['quantity'],                  # Unified: Quantity ordered
        'unit_price': df['unit_price'],              # Unified: Unit price
        'product_category': df['product_category'],  # Feature: Product category
        'to_invoice': df['subtotal'],                # Feature: Amount to invoice (using subtotal)
        'cost': df['unit_cost'],                     # Feature: Unit cost
        'sales_team': df['sales_team'],              # Feature: Sales team name
    })

    # --- Parse timestamps ---
    timestamp_columns = ['sales_date', 'last_updated']
    for col in timestamp_columns:
        df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # --- Cast numeric fields to float for Postgres NUMERIC compatibility ---
    numeric_columns = ['quantity', 'unit_price', 'to_invoice', 'cost']
    for col in numeric_columns:
        df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')

    # --- Ensure string columns are proper strings (null-safe) ---
    string_columns = ['line_item_id', 'order_id', 'sku', 'product_category', 'sales_team']
    for col in string_columns:
        df_transformed[col] = df_transformed[col].astype(str).where(df_transformed[col].notna(), None)

    logger.info(f"transform_odoo_order_lines: After transformations, shape={df_transformed.shape}")
    return df_transformed