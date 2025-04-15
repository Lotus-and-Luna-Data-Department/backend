from datetime import datetime, timedelta
import logging
from prefect import flow
import pandas as pd
from sqlalchemy.sql import text
from typing import Optional

from .extract_returns import extract_shopify_returns
from .transform_returns import transform_shopify_returns
from .load_returns import load_returns
from .validate_returns import validate_shopify_returns
from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="shopify_returns_flow")
def shopify_returns_flow(updated_at_min: Optional[str] = None, for_production: bool = False):
    """
    Orchestrates fetching Shopify refunds from updated_at >= updated_at_min,
    transforming them, and loading to CSV (test) or SQL (production).
    """
    # Determine updated_at_min based on mode
    if for_production:
        if updated_at_min is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "shopify_returns"}
                    ).fetchone()
                    if result and result[0]:
                        updated_at_min = result[0].strftime("%Y-%m-%dT%H:%M:%SZ")
                        logger.info(f"Fetched updated_at_min from DB: {updated_at_min}")
                    else:
                        updated_at_min = settings.DEFAULT_DATE
                        logger.info(f"No last_sync entry; using settings.DEFAULT_DATE={updated_at_min}")
            except Exception as e:
                logger.error(f"Failed to fetch updated_at_min from DB, using default: {e}")
                updated_at_min = settings.DEFAULT_DATE
    else:
        if updated_at_min is None:
            updated_at_min = settings.DEFAULT_DATE_TEST
            logger.info(f"Test mode: No updated_at_min provided, using settings.DEFAULT_DATE_TEST={updated_at_min}")

    # Validate ISO8601 format
    try:
        datetime.strptime(updated_at_min, "%Y-%m-%dT%H:%M:%SZ")
        logger.debug(f"Validated ISO8601 updated_at_min: {updated_at_min}")
    except ValueError as e:
        logger.error(f"Invalid updated_at_min format: {e}. Falling back to settings default.")
        updated_at_min = settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST

    logger.info("=== shopify_returns_flow: Starting with updated_at_min=%s ===", updated_at_min)

    # Subtract a 2-minute buffer from updated_at_min
    try:
        dt = datetime.strptime(updated_at_min, "%Y-%m-%dT%H:%M:%SZ")
        dt_buffered = dt - timedelta(minutes=2)
        updated_at_min = dt_buffered.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Converted updated_at_min with 2-minute buffer: {updated_at_min}")
    except Exception as e:
        logger.error(f"Failed to process updated_at_min: {e}")
        raise

    # 1) Extract
    refunds_list, refund_line_items_list = extract_shopify_returns(updated_at_min)

    if not refunds_list and not refund_line_items_list:
        logger.warning("shopify_returns_flow: No refunds or refund line items extracted. Skipping transform and load.")
        return {"status": "empty", "rows": 0}, {"status": "empty", "rows": 0}

    # 2) Transform
    refunds_df, refund_line_items_df = transform_shopify_returns(refunds_list, refund_line_items_list)
    if not isinstance(refunds_df, pd.DataFrame) or not isinstance(refund_line_items_df, pd.DataFrame):
        logger.error("shopify_returns_flow: Transformation did not return DataFrames. Skipping downstream processes.")
        return {"status": "error", "rows": 0}, {"status": "error", "rows": 0}

    # 3) Load
    returns_result = load_returns(refunds_df, refund_line_items_df, for_production=for_production)

    # 4) Validate in test mode
    if not for_production and returns_result["refunds"]["status"] == "success" and returns_result["refund_line_items"]["status"] == "success":
        try:
            validate_shopify_returns(
                refunds_csv=returns_result["refunds"]["path"],
                refund_line_items_csv=returns_result["refund_line_items"]["path"]
            )
            logger.info("shopify_returns_flow (TEST MODE): Validation completed successfully.")
        except Exception as e:
            logger.error(f"shopify_returns_flow (TEST MODE): Validation failed: {e}")
            returns_result["refunds"]["status"] = "error"
            returns_result["refund_line_items"]["status"] = "error"

    # 5) Update last_sync if production and successful
    if for_production and returns_result["refunds"]["status"] == "success" and returns_result["refund_line_items"]["status"] == "success":
        try:
            with get_engine().begin() as conn:
                # Instead of storing the original updated_at_min, take the maximum updated_at from refunds_df
                max_updated_str = None
                if ("updated_at" in refunds_df.columns) and not refunds_df.empty:
                    max_updated_str = refunds_df["updated_at"].max()  # could be a Pandas Timestamp

                if max_updated_str:
                    # If it's a Pandas Timestamp, convert it to a string first
                    if isinstance(max_updated_str, pd.Timestamp):
                        max_updated_str = max_updated_str.strftime("%Y-%m-%dT%H:%M:%SZ")

                    dt_max = datetime.strptime(max_updated_str, "%Y-%m-%dT%H:%M:%SZ")
                    dt_buffered = dt_max - timedelta(minutes=2)
                    last_sync = dt_buffered.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"Computed last_sync from max updated_at: {max_updated_str} => storing {last_sync}")
                else:
                    # If no updated_at was found, fallback to now or the original param
                    dt_now = datetime.utcnow()
                    last_sync = dt_now.strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"No max_updated_str found. Fallback last_sync={last_sync}")

                conn.execute(
                    text("INSERT INTO last_sync (type, last_updated) VALUES (:type, :ts) "
                         "ON CONFLICT (type) DO UPDATE SET last_updated = EXCLUDED.last_updated"),
                    {"type": "shopify_returns", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for shopify_returns to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for shopify_returns: {e}")

    if not for_production:
        logger.info(f"shopify_returns_flow (TEST MODE): Completed with {returns_result['refunds']['rows']} refunds and {returns_result['refund_line_items']['rows']} refund line items, saved at {returns_result['refunds']['path']} and {returns_result['refund_line_items']['path']}")
    else:
        logger.info(f"shopify_returns_flow (PRODUCTION MODE): Loaded {returns_result['refunds']['rows']} refunds and {returns_result['refund_line_items']['rows']} refund line items")
    return returns_result["refunds"], returns_result["refund_line_items"]
