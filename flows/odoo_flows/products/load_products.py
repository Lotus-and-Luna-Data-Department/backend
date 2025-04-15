import logging
import pandas as pd
import os
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def upsert_to_raw_odoo_products(df: pd.DataFrame, engine):
    """
    Perform upsert into raw_odoo_products table using raw SQL.
    Assumes 'product_id' as unique identifier.
    """
    if df.empty:
        logger.warning("No data to upsert.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient='records')
    logger.info(f"Preparing upsert for {len(records)} records.")

    columns = df.columns.tolist()
    col_names = ', '.join(columns)
    placeholders = ', '.join([f":{col}" for col in columns])

    # Exclude the primary key from the update set
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'product_id'])

    upsert_query = f"""
    INSERT INTO raw_odoo_products ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (product_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_products.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting records: {e}")
        return {"status": "error", "rows": 0}


@task
def load_odoo_products(df: pd.DataFrame, for_production: bool = False, output_dir: str = "output", filename: str = "odoo_products.csv") -> dict:
    """
    Load Odoo products to CSV (test mode) or SQL table (production mode with upsert).
    Returns a dict with status, path (if CSV), and row count.
    """
    logger.info("=== load_odoo_products: Starting load ===")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping load.")
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        # Test mode: Save to CSV
        try:
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, filename)
            df.to_csv(path, index=False)
            logger.info(f"load_odoo_products (TEST MODE): Wrote {len(df)} rows to {path}")
            return {"status": "success", "path": path, "rows": len(df)}
        except Exception as e:
            logger.error(f"Failed to save Odoo products to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}
    else:
        # Production mode: Upsert into SQL
        try:
            engine = get_engine()
            return upsert_to_raw_odoo_products(df, engine)
        except Exception as e:
            logger.error(f"Failed to load Odoo products to raw_odoo_products: {e}")
            return {"status": "error", "path": None, "rows": 0}
