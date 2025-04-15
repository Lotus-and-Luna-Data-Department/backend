# flows/odoo_flows/listings/transform_listings.py
import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_listings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw marketplace listings data into a standardized schema.

    Final schema columns:
      - listing_id
      - name
      - display_name
      - product_template
      - marketplace_id
      - marketplace_instance
      - is_listed
      - is_published
      - allow_sales_when_out_of_stock
      - created_on
      - last_updated
    """

    logger.info("=== transform_odoo_listings: starting transform ===")

    expected_columns = [
        'listing_id', 'name', 'display_name', 'product_template', 'marketplace_id',
        'marketplace_instance', 'is_listed', 'is_published',
        'allow_sales_when_out_of_stock', 'created_on', 'last_updated'
    ]

    if df.empty:
        logger.warning("No marketplace listings found; returning empty DataFrame with schema.")
        return pd.DataFrame(columns=expected_columns)

    # Map raw fields to standardized schema
    # Adjust to match the keys your endpoint actually returns
    df_transformed = pd.DataFrame({
        'listing_id': df['id'],
        'name': df['name'],
        'display_name': df['display_name'],
        'product_template': df['product_template'],
        'marketplace_id': df['marketplace_id'],
        'marketplace_instance': df['marketplace_instance'],
        'is_listed': df['is_listed'],
        'is_published': df['is_published'],
        'allow_sales_when_out_of_stock': df['allow_sales_when_out_of_stock'],
        'created_on': df['created_on'],
        'last_updated': df['last_updated'],
    })

    # Convert columns to datetime where appropriate
    date_cols = ['created_on', 'last_updated']
    for col in date_cols:
        df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # Convert boolean columns properly (some Odoo JSON might supply 'true'/'false' strings)
    bool_cols = ['is_listed', 'is_published', 'allow_sales_when_out_of_stock']
    for col in bool_cols:
        df_transformed[col] = df_transformed[col].apply(
            lambda x: bool(x) if pd.notnull(x) else None
        )

    # Ensure strings
    str_cols = ['listing_id', 'name', 'display_name', 'product_template',
                'marketplace_id', 'marketplace_instance']
    for col in str_cols:
        df_transformed[col] = df_transformed[col].astype(str).where(df_transformed[col].notna(), None)

    logger.info(f"transform_odoo_listings: Final shape={df_transformed.shape}")
    return df_transformed
