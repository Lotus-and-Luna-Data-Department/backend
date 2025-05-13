import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_stock_pickings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw stock picking data into a standardized schema.

    Final schema:
      - stock_picking_id
      - sale_order_reference
      - date_done
      - scheduled_date
      - create_date
      - last_updated
      - state
      - picking_type
    """
    logger.info("=== transform_odoo_stock_pickings: starting transform ===")

    expected_columns = [
        'stock_picking_id',
        'sale_order_reference',
        'date_done',
        'scheduled_date',
        'create_date',
        'last_updated',
        'state',
        'picking_type',
    ]

    if df.empty:
        logger.warning("Empty stock picking dataframe received. Returning empty schema.")
        return pd.DataFrame(columns=expected_columns)

    # Build the transformed DataFrame
    df_transformed = pd.DataFrame({
        'stock_picking_id': df['id'].astype(str),
        'sale_order_reference': df['sale_order_reference']
            .astype(str)
            .where(df['sale_order_reference'].notna(), None),
        'date_done': pd.to_datetime(df['date_done'], errors='coerce'),
        'scheduled_date': pd.to_datetime(df['scheduled_date'], errors='coerce'),
        'create_date': pd.to_datetime(df['create_date'], errors='coerce'),
        'last_updated': pd.to_datetime(df['write_date'], errors='coerce'),
        'state': df['state'],
        'picking_type': df['picking_type'],
    })

    # Nullify invalid timestamps
    for col in ['date_done', 'scheduled_date', 'create_date', 'last_updated']:
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # Final guarantee: cast to object and replace NaT/NaN with None
    df_transformed = df_transformed.astype(object).where(pd.notnull(df_transformed), None)

    logger.info(f"Transformed stock pickings: shape={df_transformed.shape}")
    return df_transformed
