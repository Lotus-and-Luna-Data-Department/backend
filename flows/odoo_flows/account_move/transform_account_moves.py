# flows/odoo_flows/account_moves/transform_account_moves.py

import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_account_moves(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw account.move data into standardized format.

    Final schema:
      - move_id
      - sale_order_refs
      - invoice_number_ref
      - move_type
      - state
      - amount_residual
      - amount_total
      - invoice_date_due
      - create_date
      - last_updated
    """
    logger.info("=== transform_odoo_account_moves: starting transform ===")

    expected_columns = [
        'move_id',
        'sale_order_refs',
        'invoice_number_ref',
        'move_type',
        'state',
        'amount_residual',
        'amount_total',
        'invoice_date_due',
        'create_date',
        'last_updated'
    ]

    if df.empty:
        logger.warning("Empty account moves DataFrame; returning empty schema.")
        return pd.DataFrame(columns=expected_columns)

    # Build the transformed DataFrame
    df_transformed = pd.DataFrame({
        'move_id': df['id'].astype(str),
        'sale_order_refs': df['sale_order_references']
            .apply(lambda x: ', '.join(x) if isinstance(x, list) else None),
        'invoice_number_ref': df['number'].astype(str)
            .where(df['number'].notna(), None),
        'move_type': df['move_type'],
        'state': df['state'],
        'amount_residual': df['amount_residual'],
        'amount_total': df['amount_total'],
        'invoice_date_due': pd.to_datetime(df['invoice_date_due'], errors='coerce'),
        'create_date': pd.to_datetime(df['create_date'], errors='coerce'),
        'last_updated': pd.to_datetime(df['write_date'], errors='coerce'),
    })

    # Clean null datetime fields
    for col in ['invoice_date_due', 'create_date', 'last_updated']:
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # Ensure invoice_number_ref stays None if empty
    df_transformed['invoice_number_ref'] = df_transformed['invoice_number_ref'].where(
        df_transformed['invoice_number_ref'].notna(), None
    )

    logger.info(f"transform_odoo_account_moves: final shape={df_transformed.shape}")
    return df_transformed
