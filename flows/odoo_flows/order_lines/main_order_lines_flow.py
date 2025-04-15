from datetime import datetime, timedelta
import logging
from typing import Optional
from prefect import flow
import pandas as pd
from sqlalchemy.sql import text

# Import the tasks for extraction, transformation, loading
from .extract_order_lines import extract_odoo_order_lines
from .transform_order_lines import transform_odoo_order_lines
from .load_order_lines import load_odoo_order_lines

# Import your database connection and settings
from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="odoo_order_lines_flow")
def odoo_order_lines_flow(last_sync_date: Optional[str] = None, for_production: bool = False):
    """
    Orchestrates extracting Odoo sale order lines, transforming them, and loading to CSV (test) or SQL (production).
    - If for_production=True and last_sync_date is empty, we check the DB for an existing last_sync.
    - If no DB entry is found, we fallback to settings.DEFAULT_DATE (from .env).
    - Otherwise, we use the provided or default last_sync_date.
    """
    # Determine last_sync_date based on mode
    if for_production:
        if last_sync_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "odoo_order_lines"}
                    ).fetchone()
                    if result and result[0]:
                        last_sync_date = result[0].strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"[Production] Found existing last_sync_date in DB: {last_sync_date}")
                    else:
                        last_sync_date = settings.DEFAULT_DATE
                        logger.info(f"[Production] No last_sync entry. Using settings.DEFAULT_DATE={last_sync_date}")
            except Exception as e:
                logger.error(f"Failed to fetch last_sync_date from DB; using settings.DEFAULT_DATE: {e}")
                last_sync_date = settings.DEFAULT_DATE
    else:
        if last_sync_date is None:
            last_sync_date = settings.DEFAULT_DATE_TEST
            logger.info(f"Test mode: No last_sync_date provided, using settings.DEFAULT_DATE_TEST={last_sync_date}")

    # Validate format (assuming Odoo uses "%Y-%m-%d %H:%M:%S")
    try:
        datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
        logger.debug(f"Validated last_sync_date: {last_sync_date}")
    except ValueError:
        # Try ISO8601 as a fallback since settings.DEFAULT_DATE is ISO8601
        try:
            datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"Converted ISO8601 to Odoo format: {last_sync_date}")
        except ValueError as e:
            logger.error(f"Invalid last_sync_date format: {e}. Falling back to settings default.")
            last_sync_date = settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST
            # Convert ISO8601 to Odoo format
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

    logger.info("=== odoo_order_lines_flow: Starting with last_sync_date=%s ===", last_sync_date)

    # 2) Convert ISO 860 ┌date to a datetime object (if needed) and subtract a 2-minute buffer
    try:
        if last_sync_date and "T" in last_sync_date and "Z" in last_sync_date:
            dt = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")
        else:
            dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
        dt_buffered = dt - timedelta(minutes=2)
        last_sync_date = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Converted last_sync_date for Odoo with 2-minute buffer: {last_sync_date}")
    except Exception as e:
        logger.error(f"Failed to parse/convert last_sync_date: {e}")
        raise

    # 3) Extract
    df_raw = extract_odoo_order_lines(last_sync_date=last_sync_date)
    if not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
        logger.warning("No sale order lines extracted or invalid data type. Skipping transform and load.")
        return {"status": "empty", "rows": 0}

    # 4) Transform
    df_transformed = transform_odoo_order_lines(df_raw)

    # 5) Load (CSV in test mode, upsert in production)
    result = load_odoo_order_lines(df_transformed, for_production=for_production)

    # 6) Update last_sync if production and success
    if for_production and result["status"] == "success":
        try:
            with get_engine().begin() as conn:
                # Find the maximum last_updated from df_transformed
                max_updated_str = None
                if ("last_updated" in df_transformed.columns) and not df_transformed.empty:
                    max_updated_str = df_transformed["last_updated"].max()  # Could be a Pandas Timestamp

                if max_updated_str:
                    # Handle if it's a Pandas Timestamp or string
                    if isinstance(max_updated_str, pd.Timestamp):
                        max_updated_str = max_updated_str.strftime("%Y-%m-%d %H:%M:%S")
                    elif "T" in max_updated_str and "Z" in max_updated_str:
                        # Convert ISO8601 to Odoo format if needed
                        max_updated_str = datetime.strptime(max_updated_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

                    dt_max = datetime.strptime(max_updated_str, "%Y-%m-%d %H:%M:%S")
                    dt_buffered = dt_max - timedelta(minutes=2)
                    last_sync = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"Computed last_sync from max last_updated: {max_updated_str} => storing {last_sync}")
                else:
                    # Fallback to current time if no last_updated found
                    dt_now = datetime.utcnow()
                    last_sync = dt_now.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"No max last_updated found. Fallback last_sync={last_sync}")

                conn.execute(
                    text("""
                        INSERT INTO last_sync (type, last_updated)
                        VALUES (:type, :ts)
                        ON CONFLICT (type)
                        DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """),
                    {"type": "odoo_order_lines", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for odoo_order_lines to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for odoo_order_lines: {e}")

    # 7) Return result
    if not for_production:
        logger.info(f"(TEST MODE) Completed with {result['rows']} rows; CSV path: {result.get('path')}")
    else:
        logger.info(f"(PRODUCTION) Loaded {result['rows']} rows to raw_odoo_order_lines")

    return result