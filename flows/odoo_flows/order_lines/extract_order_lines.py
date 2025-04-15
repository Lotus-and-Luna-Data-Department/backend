import logging
import pandas as pd
from typing import Optional
from prefect import task
from datetime import datetime

from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_order_lines(last_sync_date: Optional[str] = None,
                             limit: int = 1000,
                             offset: int = 0) -> pd.DataFrame:
    """
    Extract sale order lines from Odoo API with pagination.

    Args:
        last_sync_date (str, optional): Filter lines updated after this date (YYYY-MM-DD HH:MM:SS).
        limit (int, optional): Max records per request (default=100).
        offset (int, optional): Records to skip for pagination (default=0).

    Returns:
        pd.DataFrame: DataFrame containing order lines. Empty if none found or error occurs.
    """
    logger.info("=== extract_odoo_order_lines: starting extraction ===")
    logger.debug(f"Parameters: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for sale order lines")
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/sale_order_lines"
    params = {"limit": limit, "offset": offset}
    if last_sync_date:
        params["start_date"] = last_sync_date
        last_sync_dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")

    # Paginate until all records are fetched
    all_dataframes = []
    current_offset = offset
    total_records = 0
    iteration_count = 0
    out_of_range_count = 0
    max_out_of_range = 3

    while True:
        params["offset"] = current_offset
        logger.debug(f"Fetching batch: offset={current_offset}, limit={limit}, iteration={iteration_count}")

        try:
            raw_data = api.get(endpoint, params=params)
            if not raw_data:
                logger.info("No more sale order line data returned from Odoo API.")
                break

            df_batch = pd.DataFrame(raw_data)
            batch_size = len(df_batch)
            total_records += batch_size
            logger.info(f"Sale order line data extracted successfully: {batch_size} records (offset={current_offset})")

            # Log columns for debugging
            logger.info(f"Columns in batch: {list(df_batch.columns)}")

            # Filter by sales_date if last_sync_date is provided
            if last_sync_date and 'sales_date' in df_batch.columns:
                df_batch['sales_date'] = pd.to_datetime(df_batch['sales_date'], errors='coerce')
                df_batch = df_batch[df_batch['sales_date'] >= last_sync_dt]
                filtered_size = len(df_batch)
                logger.info(f"Filtered batch to {filtered_size} records based on sales_date >= {last_sync_date}")
                
                # Check if batch has no relevant data
                if filtered_size == 0:
                    out_of_range_count += 1
                    logger.info(f"No records in batch after sales_date filter; out_of_range_count={out_of_range_count}")
                    if out_of_range_count >= max_out_of_range:
                        logger.info(f"Stopping pagination after {out_of_range_count} consecutive batches with no relevant data.")
                        break
                else:
                    out_of_range_count = 0  # Reset if we found relevant data

            if batch_size == 0:
                logger.info("Empty batch received; stopping pagination.")
                break

            all_dataframes.append(df_batch)

            # Stop if fewer records than limit (end of data)
            if batch_size < limit:
                logger.info("Received fewer records than limit; stopping pagination.")
                break

            current_offset += limit
            iteration_count += 1

        except Exception as e:
            logger.error(f"Error fetching sale order line batch at offset {current_offset}: {e}")
            break

    # Combine all batches
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Sale order line data extracted successfully: {len(final_df)} records in total")
        return final_df
    else:
        logger.warning("No sale order lines extracted after pagination.")
        return pd.DataFrame()