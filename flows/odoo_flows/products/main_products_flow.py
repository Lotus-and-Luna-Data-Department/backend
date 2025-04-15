from datetime import datetime, timedelta
import logging
from prefect import flow
import pandas as pd
from sqlalchemy.sql import text
from .extract_products import extract_odoo_products
from .transform_products import transform_odoo_products
from .load_products import load_odoo_products
from base.connection import get_engine
from typing import Optional
from config import settings  # so we can access settings.DEFAULT_DATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="odoo_products_flow")
def odoo_products_flow(last_sync_date: Optional[str] = None, for_production: bool = False):
    """
    If for_production=True, tries to pull last_sync_date from the DB.
    If no entry found, fallback to settings.DEFAULT_DATE.
    Otherwise, uses whatever last_sync_date is passed in (default or user-provided).
    """
    # Determine last_sync_date based on mode
    if for_production:
        if last_sync_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "odoo_products"}
                    ).fetchone()
                    if result and result[0]:
                        last_sync_date = result[0].strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"[Production] Found existing last_sync_date in DB: {last_sync_date}")
                    else:
                        last_sync_date = settings.DEFAULT_DATE
                        logger.info(f"[Production] No last_sync entry found. Using settings.DEFAULT_DATE={last_sync_date}")
            except Exception as e:
                logger.error(f"Failed to fetch last_sync_date from DB, using settings.DEFAULT_DATE: {e}")
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

    logger.info("=== odoo_products_flow: Starting with last_sync_date=%s ===", last_sync_date)

    # Convert ISO 8601 date to Odoo format if needed and subtract a 2-minute buffer
    if last_sync_date:
        try:
            if "T" in last_sync_date and "Z" in last_sync_date:
                dt = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")
            else:
                dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
            dt_buffered = dt - timedelta(minutes=2)
            last_sync_date = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Converted last_sync_date to Odoo format with 2-minute buffer: {last_sync_date}")
        except Exception as e:
            logger.error(f"Failed to parse/convert last_sync_date: {e}")
            raise

    # 1) Extract
    df_raw = extract_odoo_products(last_sync_date=last_sync_date)
    if not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
        logger.warning("No products extracted or invalid data type. Skipping transform & load.")
        return {"status": "empty", "rows": 0}

    # 2) Transform
    df_transformed = transform_odoo_products(df_raw)

    # 3) Load
    result = load_odoo_products(df_transformed, for_production=for_production)

    # 4) Update last_sync
    if for_production and result["status"] == "success":
        try:
            # Use maximum last_updated from transformed data
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

            with get_engine().begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO last_sync (type, last_updated)
                        VALUES (:type, :ts)
                        ON CONFLICT (type)
                        DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """),
                    {"type": "odoo_products", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for odoo_products to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for odoo_products: {e}")

    return result