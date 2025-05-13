# File: flows/odoo_flows/account_payments/main_account_payments_flow.py
from datetime import datetime, timedelta
import logging
import pandas as pd
from sqlalchemy.sql import text
from typing import Optional
from prefect import flow

from .extract_account_payment import extract_odoo_account_payments
from .transform_account_payment import transform_odoo_account_payments
from .load_account_payment import load_odoo_account_payments

from base.connection import get_engine
from config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@flow(name="odoo_account_payments_flow")
def odoo_account_payments_flow(last_sync_date: Optional[str] = None, for_production: bool = False):
    """
    Orchestrates extracting Odoo account.payment records, transforming them,
    and loading to CSV (test) or raw_odoo_account_payments (production).
    Sync marker is based on the maximum last_updated from transformed records.
    """
    # === Determine last_sync_date based on mode ===
    if for_production:
        if last_sync_date is None:
            try:
                with get_engine().connect() as conn:
                    result = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :type"),
                        {"type": "odoo_account_payments"}
                    ).fetchone()
                    if result and result[0]:
                        last_sync_date = result[0].strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"[Production] Found last_sync_date in DB: {last_sync_date}")
                    else:
                        last_sync_date = settings.DEFAULT_DATE
                        logger.info(f"[Production] No last_sync entry; using DEFAULT_DATE={last_sync_date}")
            except Exception as e:
                logger.error(f"Failed to fetch last_sync_date from DB; using DEFAULT_DATE: {e}")
                last_sync_date = settings.DEFAULT_DATE
    else:
        if last_sync_date is None:
            last_sync_date = settings.DEFAULT_DATE_TEST
            logger.info(f"[Test] No last_sync_date provided; using DEFAULT_DATE_TEST={last_sync_date}")

    # === Normalize & buffer last_sync_date ===
    try:
        datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # ISO 8601 fallback
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")\
                .strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"Invalid last_sync_date format: {e}. Falling back to default.")
            last_sync_date = settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST
            last_sync_date = datetime.strptime(last_sync_date, "%Y-%m-%dT%H:%M:%SZ")\
                .strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"=== odoo_account_payments_flow: Starting with last_sync_date={last_sync_date} ===")
    # subtract 2-minute buffer
    dt0 = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=2)
    last_sync_date = dt0.strftime("%Y-%m-%d %H:%M:%S")

    # === 1) Extract ===
    df_raw = extract_odoo_account_payments(last_sync_date=last_sync_date)
    if not isinstance(df_raw, pd.DataFrame) or df_raw.empty:
        logger.warning("No payments extracted or invalid data. Skipping transform & load.")
        return {"status": "empty", "rows": 0}

    # === 2) Transform ===
    df_transformed = transform_odoo_account_payments(df_raw)

    # === 3) Load ===
    result = load_odoo_account_payments(df_transformed, for_production=for_production)

    # === 4) Update last_sync for production ===
    if for_production and result.get("status") == "success":
        try:
            max_updated = None
            if "last_updated" in df_transformed.columns and not df_transformed.empty:
                max_updated = df_transformed["last_updated"].max()

            if max_updated:
                # Ensure string format
                if isinstance(max_updated, pd.Timestamp):
                    max_str = max_updated.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    max_str = str(max_updated)
                    if "T" in max_str and "Z" in max_str:
                        max_str = datetime.strptime(max_str, "%Y-%m-%dT%H:%M:%SZ")\
                            .strftime("%Y-%m-%d %H:%M:%S")
                dt_sync = datetime.strptime(max_str, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=2)
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
                    {"type": "odoo_account_payments", "ts": last_sync}
                )
            logger.info(f"Updated last_sync for odoo_account_payments to {last_sync}")
        except Exception as e:
            logger.error(f"Failed to update last_sync for odoo_account_payments: {e}")

    # === 5) Return result ===
    if not for_production:
        logger.info(f"(TEST) Loaded {result.get('rows')} rows to CSV at {result.get('path')}")
    else:
        logger.info(f"(PRODUCTION) Loaded {result.get('rows')} rows into raw_odoo_account_payments")
    return result
