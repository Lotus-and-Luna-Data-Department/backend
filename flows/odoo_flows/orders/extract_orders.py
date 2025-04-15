import logging
import pandas as pd
from typing import Optional
from prefect import task

# Import the OdooAPI helper
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_orders(last_sync_date: Optional[str] = None,
                        limit: int = 100,
                        offset: int = 0) -> pd.DataFrame:
    """
    Extract sales orders from Odoo API, returning a pandas DataFrame.
    Implements pagination to fetch all available records by incrementing offset.

    Args:
        last_sync_date (str, optional): Filter orders updated after this date
            (YYYY-MM-DD HH:MM:SS). If None, fetch all.
        limit (int, optional): Max number of records to fetch per request (default=100).
        offset (int, optional): Initial record offset for pagination (default=0).

    Returns:
        pd.DataFrame: DataFrame of all sales orders. Could be empty if none found.
    """
    logger.info("=== extract_odoo_orders: starting extraction from Odoo ===")
    logger.debug(f"Initial parameters: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    # Initialize Odoo API
    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for sales orders")
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    # Build endpoint + base params
    endpoint = "api/sales"
    params = {
        "limit": limit,
        "offset": offset
    }
    if last_sync_date:
        params["last_sync_date"] = last_sync_date

    # Paginate until all records are fetched
    all_dataframes = []
    current_offset = offset
    total_records = 0

    while True:
        params["offset"] = current_offset
        logger.debug(f"Fetching batch: offset={current_offset}, limit={limit}")

        try:
            raw_data = api.get(endpoint, params=params)
            if not raw_data:
                logger.info("No more sales order data returned from Odoo API.")
                break

            df_batch = pd.DataFrame(raw_data)
            batch_size = len(df_batch)
            total_records += batch_size
            logger.info(f"Fetched batch: {batch_size} records (offset={current_offset})")

            if batch_size == 0:
                logger.info("Empty batch received; stopping pagination.")
                break

            all_dataframes.append(df_batch)

            # If we received fewer records than the limit, we've reached the end
            if batch_size < limit:
                logger.info("Received fewer records than limit; stopping pagination.")
                break

            current_offset += limit

        except Exception as e:
            logger.error(f"Error fetching sales order batch at offset {current_offset}: {e}")
            break

    # Combine all batches into a single DataFrame
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Sales order data extracted successfully: {len(final_df)} records in total")
        return final_df
    else:
        logger.warning("No sales orders extracted after pagination.")
        return pd.DataFrame()