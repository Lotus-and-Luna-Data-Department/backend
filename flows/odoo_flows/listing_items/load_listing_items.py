import logging
import pandas as pd
import os
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upsert_to_raw_odoo_listing_items(df: pd.DataFrame, engine):
    """
    Perform upsert into raw_odoo_listing_items table using item_id as the unique identifier.
    """
    if df.empty:
        logger.warning("No data to upsert into raw_odoo_listing_items.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient="records")
    logger.info(f"Preparing upsert for {len(records)} listing items.")

    columns = df.columns.tolist()
    col_names = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "item_id"])

    query = f"""
    INSERT INTO raw_odoo_listing_items ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (item_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_listing_items.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting listing items: {e}")
        return {"status": "error", "rows": 0}

@task
def load_odoo_listing_items(df: pd.DataFrame,
                            for_production: bool = False,
                            output_dir: str = "output",
                            filename: str = "odoo_listing_items.csv") -> dict:
    """
    Load listing items into CSV in test mode, or upsert to raw_odoo_listing_items table in production.
    """
    logger.info("=== load_odoo_listing_items: Starting load ===")

    if df.empty:
        logger.warning("DataFrame is empty; skipping load.")
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        try:
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, filename)
            df.to_csv(path, index=False)
            logger.info(f"(TEST MODE) Wrote {len(df)} rows to {path}")
            return {"status": "success", "path": path, "rows": len(df)}
        except Exception as e:
            logger.error(f"Failed to write listing items to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}
    else:
        try:
            engine = get_engine()
            return upsert_to_raw_odoo_listing_items(df, engine)
        except Exception as e:
            logger.error(f"Failed loading listing items to raw_odoo_listing_items: {e}")
            return {"status": "error", "path": None, "rows": 0}
