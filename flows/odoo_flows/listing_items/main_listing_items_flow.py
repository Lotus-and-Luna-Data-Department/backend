from datetime import datetime, timedelta
import logging
from typing import Optional
from prefect import flow
import pandas as pd
from sqlalchemy.sql import text

from .extract_listing_items import extract_odoo_listing_items
from .transform_listing_items import transform_odoo_listing_items
from .load_listing_items import load_odoo_listing_items
from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="odoo_listing_items_flow")
def odoo_listing_items_flow(last_sync_date: Optional[str] = None, for_production: bool = False):
    """
    Orchestrates extracting Odoo marketplace listing items, transforming them,
    and loading them to CSV (test) or to raw_odoo_listing_items (production).

    In production:
      - If no explicit last_sync_date is provided, attempt to fetch from DB.
      - If not found, fallback to settings.DEFAULT_DATE.
      - In all cases, subtract a 2-minute buffer.
    For testing, the default parameter is used.
    """
    # Determine last_sync_date based on mode
    if for_production:
        if last_sync_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "odoo_listing_items"}
                    ).fetchone()
                    if result and result[0]:
                        last_sync_date = result[0].strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"Fetched last_sync_date from DB: {last_sync_date}")
                    else:
                        last_sync_date = settings.DEFAULT_DATE
                        logger.info(f"No last_sync entry; using settings.DEFAULT_DATE={last_sync_date}")
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

    logger.info("=== odoo_listing_items_flow: Starting with last_sync_date=%s ===", last_sync_date)

    # 2) Convert date to datetime and subtract 2-minute buffer
    try:
        if last_sync_date and "T" in last_sync_date and "Z" in last_sync_date:
            dt = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")
        else:
            dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
        dt_buffered = dt - timedelta(minutes=2)
        last_sync_date = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Converted last_sync_date with 2-minute buffer: {last_sync_date}")
    except Exception as e:
        logger.error(f"Failed to parse/convert last_sync_date: {e}")
        raise

    # 3) Extract
    df_raw = extract_odoo_listing_items(last_sync_date=last_sync_date)
    if not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
        logger.warning("odoo_listing_items_flow: No listing items found or invalid data.")
        return {"status": "empty", "rows": 0}

    # 4) Transform
    df_transformed = transform_odoo_listing_items(df_raw)

    # 5) Load
    result = load_odoo_listing_items(df_transformed, for_production=for_production)

    # 6) Update last_sync if production and successful
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
                    {"type": "odoo_listing_items", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for odoo_listing_items to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for odoo_listing_items: {e}")

    if not for_production:
        logger.info(f"(TEST MODE) Completed with {result['rows']} rows, saved at {result.get('path')}")
    else:
        logger.info(f"(PRODUCTION) Loaded {result['rows']} rows into raw_odoo_listing_items")
    return result