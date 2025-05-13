"""
flows/shop_flows/orders/main_orders_flow.py
Robust Shopify orders ETL that:
  • works in both test & prod modes (uses Settings helper)
  • extracts → transforms → loads in CHUNK_SIZE batches
  • auto‑detects the primary‑key column coming out of the transform step
  • updates last_sync only when a batch finishes successfully
  • never keeps more than one batch in memory at a time
"""
from __future__ import annotations

from datetime import datetime, timedelta
import gc
import logging
from typing import Optional

import pandas as pd
from prefect import flow
from sqlalchemy.sql import text

from .extract_orders import extract_shopify_orders
from .transform_orders import transform_shopify_orders
from .load_orders import load_orders
from base.connection import get_engine
from config import settings  # :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CHUNK_SIZE = 500            # tune to suit memory budget
SYNC_BUFFER_MINUTES = 2     # safety buffer for incremental sync


@flow(name="shopify_orders_flow")
def shopify_orders_flow(
    start_date: Optional[str] = None,
    for_production: bool = False,
) -> tuple[dict, dict]:
    """Main flow entrypoint."""

    # ───────────────────────────┐
    # 1) Resolve effective start │
    # ───────────────────────────┘
    if for_production:
        if start_date is None:
            try:
                with get_engine().connect() as conn:
                    res = conn.execute(
                        text("SELECT last_updated FROM last_sync WHERE type = :t"),
                        {"t": "shopify_orders"},
                    ).fetchone()
                start_date = (
                    res[0].strftime("%Y-%m-%dT%H:%M:%SZ")
                    if res and res[0]
                    else settings.DEFAULT_DATE
                )
                logger.info("Using start_date from DB: %s", start_date)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Could not fetch last_sync; falling back: %s", exc)
                start_date = settings.DEFAULT_DATE
    else:
        start_date = start_date or settings.DEFAULT_DATE_TEST
        logger.info("[TEST] start_date=%s", start_date)

    # validate / buffer
    try:
        dt = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        logger.warning("Invalid ISO‑8601 start_date; falling back to defaults")
        dt = datetime.strptime(
            settings.DEFAULT_DATE if for_production else settings.DEFAULT_DATE_TEST,
            "%Y-%m-%dT%H:%M:%SZ",
        )
    dt -= timedelta(minutes=SYNC_BUFFER_MINUTES)
    start_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info("Effective start_date (buffered) = %s", start_date)

    # ───────────────────────────┐
    # 2) Extract & Transform     │
    # ───────────────────────────┘
    orders_raw, line_items_raw = extract_shopify_orders(start_date)
    if not orders_raw and not line_items_raw:
        logger.warning("No new Shopify orders; nothing to do.")
        return ({"status": "empty", "rows": 0},) * 2

    orders_df, line_items_df = transform_shopify_orders(orders_raw, line_items_raw)
    if not isinstance(orders_df, pd.DataFrame) or not isinstance(
        line_items_df, pd.DataFrame
    ):
        logger.error("Transform returned invalid DataFrames – aborting flow.")
        return ({"status": "error", "rows": 0},) * 2

    # figure out the primary key column coming out of the transform step
    ORDER_PK = "id" if "id" in orders_df.columns else "order_id"
    if ORDER_PK not in orders_df.columns:
        raise KeyError(
            f"Expected an 'id' or 'order_id' column in orders_df; found {orders_df.columns}"
        )

    total_loaded = 0
    for offset in range(0, len(orders_df), CHUNK_SIZE):
        # slice the current batch
        batch_orders = orders_df.iloc[offset : offset + CHUNK_SIZE].copy()
        batch_line_items = line_items_df[
            line_items_df["order_id"].isin(batch_orders[ORDER_PK])
        ].copy()

        logger.info(
            "Loading batch %s‑%s (orders: %s | lines: %s)",
            offset + 1,
            offset + len(batch_orders),
            len(batch_orders),
            len(batch_line_items),
        )

        result = load_orders(
            batch_orders,
            batch_line_items,
            for_production=for_production,
        )

        if (
            result["orders"]["status"] != "success"
            or result["line_items"]["status"] != "success"
        ):
            logger.error("Batch failed – halting after %s rows.", total_loaded)
            return result["orders"], result["line_items"]

        total_loaded += result["orders"]["rows"]

        # free memory ASAP
        del batch_orders, batch_line_items
        gc.collect()

    # ───────────────────────────┐
    # 3) Update last_sync (prod) │
    # ───────────────────────────┘
    if for_production and total_loaded:
        max_ts = (
            orders_df["updated_at"].max().to_pydatetime()
            if "updated_at" in orders_df.columns and not orders_df.empty
            else datetime.utcnow()
        )
        last_sync_dt = max_ts - timedelta(minutes=SYNC_BUFFER_MINUTES)
        last_sync_str = last_sync_dt.strftime("%Y-%m-%d %H:%M:%S")

        try:
            with get_engine().begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO last_sync (type, last_updated)
                        VALUES (:t, :ts)
                        ON CONFLICT (type)
                        DO UPDATE SET last_updated = EXCLUDED.last_updated
                        """
                    ),
                    {"t": "shopify_orders", "ts": last_sync_str},
                )
            logger.info("last_sync updated to %s", last_sync_str)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Could not update last_sync: %s", exc)

    logger.info(
        "[%s] shopify_orders_flow completed – %s orders loaded in %s‑row chunks",
        "PROD" if for_production else "TEST",
        total_loaded,
        CHUNK_SIZE,
    )
    return ({"status": "success", "rows": total_loaded},) * 2
