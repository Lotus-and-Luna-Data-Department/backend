import logging
import pandas as pd
import os
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upsert_to_raw_odoo_listings(df: pd.DataFrame, engine):
    """
    Perform an upsert into raw_odoo_listings using the listing_id as the unique key.
    """
    if df.empty:
        logger.warning("No data to upsert into raw_odoo_listings.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient='records')
    logger.info(f"Preparing upsert for {len(records)} listings records.")

    columns = df.columns.tolist()
    col_names = ', '.join(columns)
    placeholders = ', '.join([f":{col}" for col in columns])
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'listing_id'])

    upsert_query = f"""
    INSERT INTO raw_odoo_listings ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (listing_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_listings.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting listings into raw_odoo_listings: {e}")
        return {"status": "error", "rows": 0}

@task
def load_odoo_listings(df: pd.DataFrame,
                       for_production: bool = False,
                       output_dir: str = "output",
                       filename: str = "odoo_listings.csv") -> dict:
    """
    Load marketplace listings to CSV in test mode or perform upsert to the raw_odoo_listings table
    in production mode.

    Args:
        df (pd.DataFrame): Data to load.
        for_production (bool): If True, upsert to SQL; else write to CSV.
        output_dir (str): Directory path for test CSV output.
        filename (str): Name of the test CSV file.

    Returns:
        dict: { status, path, rows } or { status, rows } in production.
    """
    logger.info("=== load_odoo_listings: Starting load ===")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping load step.")
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        try:
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, filename)
            df.to_csv(path, index=False)
            logger.info(f"(TEST MODE) Wrote {len(df)} listings to {path}")
            return {"status": "success", "path": path, "rows": len(df)}
        except Exception as e:
            logger.error(f"Failed writing listings to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}
    else:
        try:
            engine = get_engine()
            return upsert_to_raw_odoo_listings(df, engine)
        except Exception as e:
            logger.error(f"Failed to load listings into raw_odoo_listings: {e}")
            return {"status": "error", "path": None, "rows": 0}
