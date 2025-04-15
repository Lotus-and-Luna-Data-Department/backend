import logging
import pandas as pd
import os
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upsert_to_raw_odoo_orders(df: pd.DataFrame, engine):
    """
    Perform upsert into raw_odoo_orders table using raw SQL.
    Assumes 'order_id' as unique identifier.
    """
    if df.empty:
        logger.warning("No data to upsert.")
        return {"status": "empty", "rows": 0}

    # Exclude tags from raw_odoo_orders
    df_orders = df.drop(columns=["tags"], errors="ignore")
    records = df_orders.to_dict(orient="records")
    logger.info(f"Preparing upsert for {len(records)} records into raw_odoo_orders.")

    columns = df_orders.columns.tolist()
    col_names = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "order_id"])

    upsert_query = f"""
    INSERT INTO raw_odoo_orders ({col_names})
    VALUES ({placeholders})
    ON CONFLICT (order_id) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into raw_odoo_orders.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting records into raw_odoo_orders: {e}")
        return {"status": "error", "rows": 0}

def upsert_to_odoo_order_tags(df: pd.DataFrame, engine):
    """
    Perform upsert into odoo_order_tags table using raw SQL.
    """
    if df.empty or "tags" not in df.columns:
        logger.warning("No tags to upsert.")
        return {"status": "empty", "rows": 0}

    tag_records = []
    for _, row in df.iterrows():
        order_id = row["order_id"]
        tags = row["tags"] if isinstance(row["tags"], list) else []
        for tag in tags:
            if tag:  # Skip empty tags
                tag_records.append({"order_id": order_id, "tag": tag})

    if not tag_records:
        logger.warning("No valid tags to upsert.")
        return {"status": "empty", "rows": 0}

    upsert_query = """
    INSERT INTO odoo_order_tags (order_id, tag)
    VALUES (:order_id, :tag)
    ON CONFLICT (order_id, tag) DO NOTHING;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), tag_records)
        logger.info(f"Upserted {len(tag_records)} tags into odoo_order_tags.")
        return {"status": "success", "rows": len(tag_records)}
    except Exception as e:
        logger.error(f"Failed upserting tags into odoo_order_tags: {e}")
        return {"status": "error", "rows": 0}

@task
def load_odoo_orders(df: pd.DataFrame, for_production: bool = False, output_dir: str = "output", filename: str = "odoo_orders.csv") -> dict:
    """
    Load Odoo orders to CSVs (test mode) or SQL tables (production mode with upsert).
    In test mode, generates:
      - odoo_orders.csv: Orders with tags as a comma-separated string.
      - odoo_order_tags.csv: Order ID and tag pairs.
    In production, loads into raw_odoo_orders and odoo_order_tags.
    """
    logger.info("=== load_odoo_orders: Starting load ===")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping load.")
        return {"status": "empty", "path": None, "rows": 0}

    if not for_production:
        # Test mode: Generate two CSVs
        # 1. odoo_orders.csv (with tags as comma-separated string)
        df_orders = df.copy()
        if "tags" in df_orders.columns:
            df_orders["tags"] = df_orders["tags"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        try:
            os.makedirs(output_dir, exist_ok=True)
            orders_path = os.path.join(output_dir, filename)
            df_orders.to_csv(orders_path, index=False)
            logger.info(f"load_odoo_orders (TEST MODE): Wrote {len(df)} rows to {orders_path}")
        except Exception as e:
            logger.error(f"Failed to save Odoo orders to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}

        # 2. odoo_order_tags.csv (order_id and tag pairs)
        tag_records = []
        for _, row in df.iterrows():
            order_id = row["order_id"]
            tags = row["tags"] if isinstance(row["tags"], list) else []
            for tag in tags:
                if tag:  # Skip empty tags
                    tag_records.append({"order_id": order_id, "tag": tag})

        df_tags = pd.DataFrame(tag_records)
        tags_path = os.path.join(output_dir, "odoo_order_tags.csv")
        try:
            if not df_tags.empty:
                df_tags.to_csv(tags_path, index=False)
                logger.info(f"load_odoo_orders (TEST MODE): Wrote {len(df_tags)} tag rows to {tags_path}")
            else:
                logger.info("No tags to write to odoo_order_tags.csv")
        except Exception as e:
            logger.error(f"Failed to save Odoo order tags to CSV: {e}")
            return {"status": "error", "path": None, "rows": 0}

        return {
            "status": "success",
            "path": orders_path,
            "tags_path": tags_path,
            "rows": len(df),
            "tags_rows": len(df_tags)
        }
    else:
        try:
            engine = get_engine()
            # Upsert into raw_odoo_orders (without tags)
            orders_result = upsert_to_raw_odoo_orders(df, engine)
            if orders_result["status"] != "success":
                return orders_result

            # Upsert into odoo_order_tags
            tags_result = upsert_to_odoo_order_tags(df, engine)
            return {
                "status": "success",
                "path": None,
                "rows": orders_result["rows"],
                "tags_rows": tags_result["rows"]
            }
        except Exception as e:
            logger.error(f"Failed to load Odoo orders: {e}")
            return {"status": "error", "path": None, "rows": 0}