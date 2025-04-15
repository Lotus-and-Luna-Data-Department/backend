import logging
import pandas as pd
from typing import Optional
from datetime import datetime
from prefect import task

# Import the OdooAPI helper
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_products(last_sync_date: Optional[str] = None, limit: int = 100, offset: int = 0) -> pd.DataFrame:
    """
    Extract product variants from Odoo API, returning a pandas DataFrame.
    Implements pagination to fetch all available records. If last_sync_date is None,
    a default wide date range is used to fetch all products.

    Args:
        last_sync_date (str, optional): Filter products updated after this date
            (YYYY-MM-DD HH:MM:SS). If None, a default wide range is applied.
        limit (int, optional): Max number of records to fetch per request (default=100).
        offset (int, optional): Initial record offset for pagination (default=0).

    Returns:
        pd.DataFrame: DataFrame of all product variants. Could be empty if none found.
    """
    logger.info("=== extract_odoo_products: starting extraction from Odoo ===")
    logger.debug(f"Initial parameters: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    # Initialize Odoo API
    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for product variants")
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    # Build endpoint
    endpoint = "/api/products"
    params = {
        "limit": limit,
        "offset": offset
    }

    # Set start_date and end_date appropriately
    if last_sync_date:
        try:
            start_dt = datetime.strptime(last_sync_date, "%Y-%m-%d %H:%M:%S")
            params["start_date"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Using last_sync_date as start_date: {start_dt}")
            end_dt = datetime.now()
            params["end_date"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Using end_date: {end_dt}")
        except ValueError as e:
            logger.error(f"Invalid last_sync_date format: {last_sync_date} | Error: {str(e)}")
            raise ValueError("Invalid date format for last_sync_date")
    else:
        # Default wide date range if no last_sync_date provided
        params["start_date"] = "2023-01-01 00:00:00"
        params["end_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Using default broad date range: {params['start_date']} to {params['end_date']}")

    # Paginate to fetch all records
    all_dataframes = []
    current_offset = offset
    total_records = 0
    batch_number = 1

    while True:
        params["offset"] = current_offset
        logger.info(f"Fetching batch #{batch_number} with offset={current_offset}, limit={limit}")

        try:
            response = api.get(endpoint, params=params)

            # Check if response is a list (direct list of products)
            if not isinstance(response, list):
                logger.warning("Invalid response format; expected a list of products. Stopping.")
                break

            # If the response is empty, stop pagination
            if not response:
                logger.info("No more product data returned from Odoo API. Stopping.")
                break

            # Convert the list of dictionaries to a DataFrame
            df_batch = pd.DataFrame(response)
            batch_size = len(df_batch)
            total_records += batch_size
            logger.info(f"Fetched batch #{batch_number}: {batch_size} records")

            if batch_size == 0:
                logger.info("Empty batch received; stopping pagination.")
                break

            all_dataframes.append(df_batch)

            if batch_size < limit:
                logger.info("Received fewer records than limit; assuming final batch. Stopping pagination.")
                break

            current_offset += limit
            batch_number += 1

        except Exception as e:
            logger.error(f"Error fetching product batch at offset {current_offset}: {e}")
            break

    # Combine all batches into a single DataFrame
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Product data extraction completed: {len(final_df)} total records extracted")
        return final_df
    else:
        logger.warning("No products extracted after pagination.")
        return pd.DataFrame()
