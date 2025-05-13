# flows/odoo_flows/account_moves/main_account_moves_flow.py
from datetime import datetime, timedelta
import logging
import pandas as pd
from sqlalchemy.sql import text
from typing import Optional
from prefect import flow

from .extract_account_moves import extract_odoo_account_moves
from .transform_account_moves import transform_odoo_account_moves
from .load_account_moves import load_odoo_account_moves

from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="odoo_account_moves_flow")
def odoo_account_moves_flow(last_sync_date: Optional[str] = None, for_production: bool = False):
    # === Resolve last_sync_date ===
    if for_production:
        if last_sync_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "odoo_account_moves"}
                    ).fetchone()
                    if result and result[0]:
                        last_sync_date = result[0].strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"[Production] Using last_sync_date from DB: {last_sync_date}")
                    else:
                        last_sync_date = settings.DEFAULT_DATE
                        logger.info(f"[Production] No DB entry. Using DEFAULT_DATE={last_sync_date}")
            except Exception as e:
                logger.error(f"Error fetching last_sync_date from DB. Using fallback: {e}")
                last_sync_date = settings.DEFAULT_DATE
    else:
        if last_sync_date is None:
            last_sync_date = settings.DEFAULT_DATE_TEST
            logger.info(f"[Test] Using DEFAULT_DATE_TEST={last_sync_date}")

    # === Normalize to Odoo format ===
    try:
        datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logger.error(f"Invalid format: {e}. Falling back to settings default.")
            last_sync_date = settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Starting account moves flow with last_sync_date={last_sync_date}")
    dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=2)
    last_sync_date = dt.strftime("%Y-%m-%d %H:%M:%S")

    # === 1. Extract ===
    df_raw = extract_odoo_account_moves(last_sync_date=last_sync_date)
    if df_raw.empty:
        logger.warning("No data returned. Skipping transform/load.")
        return {"status": "empty", "rows": 0}

    # === 2. Transform ===
    df_transformed = transform_odoo_account_moves(df_raw)

    # === 3. Load ===
    result = load_odoo_account_moves(df_transformed, for_production=for_production)

    # === 4. Sync Tracking ===
    if for_production and result["status"] == "success":
        try:
            max_updated_str = None
            if "last_updated" in df_transformed.columns and not df_transformed.empty:
                max_updated_str = df_transformed["last_updated"].max()

            if max_updated_str:
                if isinstance(max_updated_str, pd.Timestamp):
                    max_updated_str = max_updated_str.strftime("%Y-%m-%d %H:%M:%S")
                elif "T" in str(max_updated_str) and "Z" in str(max_updated_str):
                    max_updated_str = datetime.strptime(max_updated_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

                dt_sync = datetime.strptime(max_updated_str, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=2)
                last_sync = dt_sync.strftime("%Y-%m-%d %H:%M:%S")
            else:
                last_sync = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            with get_engine().begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO last_sync (type, last_updated)
                        VALUES (:type, :ts)
                        ON CONFLICT (type)
                        DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """),
                    {"type": "odoo_account_moves", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for odoo_account_moves to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for odoo_account_moves: {e}")

    if not for_production:
        logger.info(f"(TEST MODE) Loaded {result['rows']} rows to CSV at {result.get('path')}")
    else:
        logger.info(f"(PRODUCTION) Loaded {result['rows']} rows to raw_odoo_account_moves")
    return result
