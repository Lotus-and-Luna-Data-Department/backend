import logging
import pandas as pd
import os
from typing import Dict
from prefect import task
from base.connection import get_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def upsert_to_table(df: pd.DataFrame, table_name: str, unique_key: str, engine) -> Dict:
    """
    Perform upsert into the specified SQL table using raw SQL.
    
    Args:
        df (pd.DataFrame): Data to upsert.
        table_name (str): Target SQL table name.
        unique_key (str): Unique column used for conflict resolution.
        engine: SQLAlchemy engine for database connection.

    Returns:
        Dict: Status and row count.
    """
    if df.empty:
        logger.warning(f"No data to upsert into {table_name}.")
        return {"status": "empty", "rows": 0}

    records = df.to_dict(orient='records')
    logger.info(f"Preparing upsert for {len(records)} records in {table_name}.")

    columns = df.columns.tolist()
    col_names = ', '.join(columns)
    placeholders = ', '.join([f":{col}" for col in columns])  # Changed to :name syntax

    # Exclude the primary key from updates
    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key])

    upsert_query = f"""
    INSERT INTO {table_name} ({col_names})
    VALUES ({placeholders})
    ON CONFLICT ({unique_key}) DO UPDATE
    SET {updates};
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(upsert_query), records)
        logger.info(f"Upserted {len(records)} records into {table_name}.")
        return {"status": "success", "rows": len(records)}
    except Exception as e:
        logger.error(f"Failed upserting records into {table_name}: {e}")
        return {"status": "error", "rows": 0}


@task
def load_orders(orders_df: pd.DataFrame, line_items_df: pd.DataFrame, for_production: bool = False, output_dir: str = "output") -> Dict[str, Dict]:
    """
    Load Shopify orders and line items to CSV (test mode) or SQL tables (production mode with upsert).
    
    Args:
        orders_df (pd.DataFrame): DataFrame containing orders.
        line_items_df (pd.DataFrame): DataFrame containing line items.
        for_production (bool): If True, load to SQL; if False, save to CSV.
        output_dir (str): Directory to save CSV files.

    Returns:
        Dict[str, Dict]: Status, paths (if CSV), and row counts for orders and line items.
    """
    logger.info("=== load_orders: Starting load ===")

    result = {
        "orders": {"status": "empty", "path": None, "rows": 0},
        "line_items": {"status": "empty", "path": None, "rows": 0}
    }

    if orders_df.empty and line_items_df.empty:
        logger.warning("Both orders and line items DataFrames are empty. Skipping load.")
        return result

    if not for_production:
        # Test mode: Save to CSV
        try:
            os.makedirs(output_dir, exist_ok=True)
            orders_csv = os.path.join(output_dir, "shopify_orders.csv")
            line_items_csv = os.path.join(output_dir, "shopify_order_line_items.csv")

            orders_df.to_csv(orders_csv, index=False)
            result["orders"]["status"] = "success"
            result["orders"]["path"] = orders_csv
            result["orders"]["rows"] = len(orders_df)
            logger.info(f"load_orders (TEST MODE): Wrote {len(orders_df)} orders to {orders_csv}")

            line_items_df.to_csv(line_items_csv, index=False)
            result["line_items"]["status"] = "success"
            result["line_items"]["path"] = line_items_csv
            result["line_items"]["rows"] = len(line_items_df)
            logger.info(f"load_orders (TEST MODE): Wrote {len(line_items_df)} line items to {line_items_csv}")
        except Exception as e:
            logger.error(f"Failed to save Shopify orders or line items to CSV: {e}")
            result["orders"]["status"] = "error"
            result["line_items"]["status"] = "error"
    else:
        # Production mode: Upsert into SQL
        try:
            engine = get_engine()

            result["orders"] = upsert_to_table(orders_df, "raw_shopify_orders", "order_id", engine)
            result["line_items"] = upsert_to_table(line_items_df, "raw_shopify_order_lines", "line_item_id", engine)

        except Exception as e:
            logger.error(f"Failed to load Shopify orders or line items to SQL: {e}")
            result["orders"]["status"] = "error"
            result["line_items"]["status"] = "error"

    return result