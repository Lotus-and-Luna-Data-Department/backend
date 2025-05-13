# flows/odoo_flows/stock_pickings/load_stock_pickings.py
import logging
import pandas as pd
import os
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upsert_to_raw_odoo_stock_pickings(df: pd.DataFrame, engine):
    if df.empty:
        logger.warning("No data to upsert into raw_odoo_stock_pickings.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient='records')
    logger.info(f"Preparing upsert for {len(records)} stock picking records.")

    columns = df.columns.tolist()
    col_names = ', '.join(columns)
    placeholders = ', '.join([f":{col}" for col in columns])
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'stock_picking_id'])

    upsert_query = f"""
    INSERT INTO raw_odoo_stock_pickings ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (stock_picking_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_stock_pickings.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting stock pickings into raw_odoo_stock_pickings: {e}")
        return {"status": "error", "rows": 0}

@task
def load_odoo_stock_pickings(df: pd.DataFrame,
                             for_production: bool = False,
                             output_dir: str = "output",
                             filename: str = "odoo_stock_pickings.csv") -> dict:
    logger.info("=== load_odoo_stock_pickings: Starting load ===")

    if df.empty:
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        logger.info(f"(TEST MODE) Wrote {len(df)} stock pickings to {path}")
        return {"status": "success", "path": path, "rows": len(df)}

    try:
        engine = get_engine()
        return upsert_to_raw_odoo_stock_pickings(df, engine)
    except Exception as e:
        logger.error(f"Failed to load stock pickings into raw_odoo_stock_pickings: {e}")
        return {"status": "error", "path": None, "rows": 0}
