from datetime import datetime
import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw Odoo orders DataFrame to match the final schema.
    It returns a DataFrame with the following fields:
      - order_id
      - create_date
      - last_updated (renamed from write_date)
      - sales_date
      - date_order (mapped from order_date)
      - delivery_date
      - delivery_address
      - sales_team
      - sales_person
      - amount_total
      - payment_terms
      - state
      - invoice_status
      - customer
      - shipping_policy
    Timestamp columns are converted robustly; missing values become None.
    Note: 'tags' is kept as a list for the load step but excluded from the final schema.
    """
    expected_columns = [
        "order_id", "create_date", "last_updated", "sales_date", "date_order",
        "delivery_date", "delivery_address", "sales_team", "sales_person",
        "amount_total", "payment_terms", "state", "invoice_status",
        "customer", "shipping_policy"
    ]
    
    if df.empty:
        logger.warning("No Odoo orders found; returning empty DataFrame with expected schema.")
        return pd.DataFrame(columns=expected_columns)
    
    # Build the new DataFrame with the desired fields.
    df_transformed = pd.DataFrame({
        "order_id": df["order_reference"],  # CHANGED: use new order_reference
        "create_date": df["create_date"],
        "last_updated": df["write_date"],
        "sales_date": df["sales_date"],
        "date_order": df["order_date"],
        "delivery_date": df["delivery_date"],
        "delivery_address": df["delivery_address"],
        "sales_team": df["sales_team"],
        "sales_person": df["salesperson"],
        "amount_total": df["amount_total"],
        "payment_terms": df["payment_terms"],
        "tags": df["tags"],  # Keep as list for load step
        "state": df["status"],
        "invoice_status": df["invoice_status"],
        "customer": df["customer"],
        "shipping_policy": df["shipping_policy"]
    })
    
    # Process timestamp columns robustly.
    timestamp_columns = ['sales_date', 'last_updated']
    for col in timestamp_columns:
        df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)
    
    # Cast numeric fields.
    numeric_columns = ["amount_total"]
    for col in numeric_columns:
        df_transformed[col] = pd.to_numeric(df_transformed[col], errors="coerce")
    
    # Ensure all remaining text fields are cast to string; leave missing values as None.
    string_columns = [
        "order_id", "delivery_address", "sales_team", "sales_person",
        "payment_terms", "state", "invoice_status", "customer", "shipping_policy"
    ]
    for col in string_columns:
        df_transformed[col] = df_transformed[col].astype(str).where(df_transformed[col].notna(), None)
    
    logger.info(f"transform_odoo_orders: After transformations, shape={df_transformed.shape}")
    return df_transformed
