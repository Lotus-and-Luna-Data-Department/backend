import logging
import pandas as pd
from typing import Optional
from prefect import task
import os
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upsert_to_raw_odoo_order_lines(df: pd.DataFrame, engine):
    """
    Perform upsert into raw_odoo_order_lines table using raw SQL.
    Assumes 'line_item_id' as the unique identifier.
    """
    if df.empty:
        logger.warning("No data to upsert.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient='records')
    logger.info(f"Preparing upsert for {len(records)} records.")

    columns = df.columns.tolist()
    col_names = ', '.join(columns)
    placeholders = ', '.join([f":{col}" for col in columns])

    # Use "line_item_id" as the unique key in the ON CONFLICT clause
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'line_item_id'])

    upsert_query = f"""
    INSERT INTO raw_odoo_order_lines ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (line_item_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_order_lines.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting records: {e}")
        return {"status": "error", "rows": 0}

@task
def load_odoo_order_lines(df: pd.DataFrame, for_production: bool = False, output_dir: str = "output", filename: str = "odoo_sale_order_lines.csv") -> dict:
    """
    Load sale order lines to CSV (test mode) or SQL table (production mode with upsert).
    """
    logger.info("=== load_odoo_order_lines: Starting load ===")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping load.")
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        try:
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            df.to_csv(output_path, index=False)
            logger.info(f"load_odoo_order_lines (TEST MODE): Wrote {len(df)} rows to {output_path}")
            return {"status": "success", "path": output_path, "rows": len(df)}
        except Exception as e:
            logger.error(f"Failed to save sale order lines to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}
    else:
        try:
            engine = get_engine()
            return upsert_to_raw_odoo_order_lines(df, engine)
        except Exception as e:
            logger.error(f"Failed to load sale order lines to raw_odoo_order_lines: {e}")
            return {"status": "error", "path": None, "rows": 0}
