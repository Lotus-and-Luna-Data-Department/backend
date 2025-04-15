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
def load_returns(refunds_df: pd.DataFrame, refund_line_items_df: pd.DataFrame, for_production: bool = False, output_dir: str = "output") -> Dict[str, Dict]:
    """
    Load Shopify refunds and refund line items to CSV (test mode) or SQL tables (production mode with upsert).
    
    Args:
        refunds_df (pd.DataFrame): DataFrame containing refunds.
        refund_line_items_df (pd.DataFrame): DataFrame containing refund line items.
        for_production (bool): If True, load to SQL; if False, save to CSV.
        output_dir (str): Directory to save CSV files.

    Returns:
        Dict[str, Dict]: Status, paths (if CSV), and row counts for refunds and refund line items.
    """
    logger.info("=== load_returns: Starting load ===")

    result = {
        "refunds": {"status": "empty", "path": None, "rows": 0},
        "refund_line_items": {"status": "empty", "path": None, "rows": 0}
    }

    if refunds_df.empty and refund_line_items_df.empty:
        logger.warning("Both refunds and refund line items DataFrames are empty. Skipping load.")
        return result

    if not for_production:
        # Test mode: Save to CSV
        try:
            os.makedirs(output_dir, exist_ok=True)
            refunds_csv = os.path.join(output_dir, "shopify_refunds.csv")
            refund_line_items_csv = os.path.join(output_dir, "shopify_refund_line_items.csv")

            refunds_df.to_csv(refunds_csv, index=False)
            result["refunds"]["status"] = "success"
            result["refunds"]["path"] = refunds_csv
            result["refunds"]["rows"] = len(refunds_df)
            logger.info(f"load_returns (TEST MODE): Wrote {len(refunds_df)} refunds to {refunds_csv}")

            refund_line_items_df.to_csv(refund_line_items_csv, index=False)
            result["refund_line_items"]["status"] = "success"
            result["refund_line_items"]["path"] = refund_line_items_csv
            result["refund_line_items"]["rows"] = len(refund_line_items_df)
            logger.info(f"load_returns (TEST MODE): Wrote {len(refund_line_items_df)} refund line items to {refund_line_items_csv}")
        except Exception as e:
            logger.error(f"Failed to save Shopify refunds or refund line items to CSV: {e}")
            result["refunds"]["status"] = "error"
            result["refund_line_items"]["status"] = "error"
    else:
        # Production mode: Upsert into SQL
        try:
            engine = get_engine()

            result["refunds"] = upsert_to_table(refunds_df, "raw_shopify_refunds", "refund_id", engine)
            result["refund_line_items"] = upsert_to_table(refund_line_items_df, "raw_shopify_refund_lines", "refund_line_item_id", engine)

        except Exception as e:
            logger.error(f"Failed to load Shopify refunds or refund line items to SQL: {e}")
            result["refunds"]["status"] = "error"
            result["refund_line_items"]["status"] = "error"

    return result