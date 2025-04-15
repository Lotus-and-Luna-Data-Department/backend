from datetime import datetime, timedelta
import logging
from prefect import flow
import pandas as pd
from sqlalchemy.sql import text
from typing import Optional

from .extract_orders import extract_shopify_orders
from .transform_orders import transform_shopify_orders
from .load_orders import load_orders
from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="shopify_orders_flow")
def shopify_orders_flow(start_date: Optional[str] = None, for_production: bool = False):
    """
    Orchestrates extracting Shopify orders from a start_date,
    transforming them, and loading to CSV (test) or SQL (production).
    """
    # Determine start_date based on mode
    if for_production:
        if start_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "shopify_orders"}
                    ).fetchone()
                    if result and result[0]:
                        start_date = result[0].strftime("%Y-%m-%dT%H:%M:%SZ")
                        logger.info(f"Fetched start_date from DB: {start_date}")
                    else:
                        start_date = settings.DEFAULT_DATE
                        logger.info(f"No last_sync entry; using settings.DEFAULT_DATE={start_date}")
            except Exception as e:
                logger.error(f"Failed to fetch start_date from DB, using default: {e}")
                start_date = settings.DEFAULT_DATE
    else:
        if start_date is None:
            start_date = settings.DEFAULT_DATE_TEST
            logger.info(f"Test mode: No start_date provided, using settings.DEFAULT_DATE_TEST={start_date}")

    # Validate ISO8601 format
    try:
        datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        logger.debug(f"Validated ISO8601 start_date: {start_date}")
    except ValueError as e:
        logger.error(f"Invalid start_date format: {e}. Falling back to settings default.")
        start_date = settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST

    logger.info("=== shopify_orders_flow: Starting with start_date=%s ===", start_date)

    # Subtract a 2-minute buffer from start_date
    try:
        dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        dt_buffered = dt - timedelta(minutes=2)
        start_date = dt_buffered.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Converted start_date with 2-minute buffer: {start_date}")
    except Exception as e:
        logger.error(f"Failed to process start_date: {e}")
        raise

    # 1) Extract
    orders_list, line_items_list = extract_shopify_orders(start_date)

    if not orders_list and not line_items_list:
        logger.warning("shopify_orders_flow: No orders or line items extracted. Skipping transform and load.")
        return {"status": "empty", "rows": 0}, {"status": "empty", "rows": 0}

    # 2) Transform
    orders_df, line_items_df = transform_shopify_orders(orders_list, line_items_list)

    if not isinstance(orders_df, pd.DataFrame) or not isinstance(line_items_df, pd.DataFrame):
        logger.error("shopify_orders_flow: Transformation did not return DataFrames. Skipping downstream processes.")
        return {"status": "error", "rows": 0}, {"status": "error", "rows": 0}

    # 3) Load
    orders_result = load_orders(orders_df, line_items_df, for_production=for_production)

    # 4) Update last_sync if production and both orders and line items load successfully
    if for_production and orders_result["orders"]["status"] == "success" and orders_result["line_items"]["status"] == "success":
        try:
            with get_engine().begin() as conn:
                # Instead of storing the original start_date, we find the maximum updated_at from orders_df
                max_updated_str = None
                if ("updated_at" in orders_df.columns) and not orders_df.empty:
                    max_updated_str = orders_df["updated_at"].max()  # could be a Pandas Timestamp

                if max_updated_str:
                    # If it's a Pandas Timestamp, convert it to a string first
                    if isinstance(max_updated_str, pd.Timestamp):
                        max_updated_str = max_updated_str.strftime("%Y-%m-%dT%H:%M:%SZ")

                    dt_max = datetime.strptime(max_updated_str, "%Y-%m-%dT%H:%M:%SZ")
                    dt_buffered = dt_max - timedelta(minutes=2)
                    last_sync = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"Computed last_sync from max updated_at: {max_updated_str} => storing {last_sync}")
                else:
                    # Fallback: if no updated_at found, just store the original 'start_date' we used
                    # or store now(). We'll do a safe fallback:
                    dt_now = datetime.utcnow()
                    last_sync = dt_now.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"No max_updated_str found. Fallback last_sync={last_sync}")

                conn.execute(
                    text("INSERT INTO last_sync (type, last_updated) VALUES (:type, :ts) "
                         "ON CONFLICT (type) DO UPDATE SET last_updated = EXCLUDED.last_updated"),
                    {"type": "shopify_orders", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for shopify_orders to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for shopify_orders: {e}")

    if not for_production:
        logger.info(f"shopify_orders_flow (TEST MODE): Completed with {orders_result['orders']['rows']} orders and {orders_result['line_items']['rows']} line items, saved at {orders_result['orders']['path']} and {orders_result['line_items']['path']}")
    else:
        logger.info(f"shopify_orders_flow (PRODUCTION MODE): Loaded {orders_result['orders']['rows']} orders and {orders_result['line_items']['rows']} line items")
    return orders_result["orders"], orders_result["line_items"]
