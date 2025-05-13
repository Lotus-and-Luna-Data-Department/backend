# flows/odoo_flows/account_payments/transform_account_payments.py
import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_account_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw account.payment data into standardized format.

    Final schema:
      - payment_id
      - payment_move_id
      - invoice_move_id
      - invoice_number_ref
      - amount
      - create_date
      - last_updated
    """
    logger.info("=== transform_odoo_account_payments: starting transform ===")

    expected_columns = [
        'payment_id', 'payment_move_id', 'invoice_move_id',
        'invoice_number_ref', 'amount', 'create_date', 'last_updated'
    ]

    if df.empty:
        logger.warning(
            "No account payments data found; returning empty DataFrame with expected schema."
        )
        return pd.DataFrame(columns=expected_columns)

    # Ensure every list is exploded, including empty lists -> keep a None row
    df = df.copy()
    df['invoice_move_ids'] = df['invoice_move_ids'].apply(
        lambda x: x if isinstance(x, list) and x else [None]
    )
    df_exp = df.explode('invoice_move_ids')

    # Build transformed DataFrame
    df_transformed = pd.DataFrame({
        'payment_id': df_exp['payment_id'].astype(str),
        'payment_move_id': df_exp['payment_move_id'].astype(str),
        'invoice_move_id': df_exp['invoice_move_ids'].astype(str),
        'invoice_number_ref': df_exp['invoice_number_ref'].astype(str).where(
            df_exp['invoice_number_ref'].notna(), None
        ),
        'amount': pd.to_numeric(df_exp['amount'], errors='coerce'),
        'create_date': pd.to_datetime(df_exp['create_date'], errors='coerce'),
        'last_updated': pd.to_datetime(df_exp['write_date'], errors='coerce'),
    })

    # Nullify invalid timestamps
    for col in ['create_date', 'last_updated']:
        df_transformed[col] = df_transformed[col].where(
            df_transformed[col].notna(), None
        )

    # Final guarantee: replace NaN/NaT with None and cast to object
    df_transformed = df_transformed.astype(object).where(pd.notnull(df_transformed), None)

    logger.info(f"transform_odoo_account_payments: final shape={df_transformed.shape}")
    return df_transformed
