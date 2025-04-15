# flows/odoo_flows/listing_items/extract_listing_items.py
import logging
import pandas as pd
from typing import Optional
from prefect import task
from flows.odoo_flows.odoo_utils import OdooAPI  # your existing Odoo API helper

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_listing_items(last_sync_date: Optional[str] = None,
                               limit: int = 100,
                               offset: int = 0) -> pd.DataFrame:
    """
    Extract marketplace listing items from Odoo's /api/marketplace-listing-items endpoint,
    returning a pandas DataFrame. Paginates until no more records are found.

    Args:
        last_sync_date (str): Filter by write_date >= last_sync_date if given.
        limit (int): Page size for each request. Default=100.
        offset (int): Starting offset for pagination.
    """
    logger.info("=== extract_odoo_listing_items: Starting extraction ===")
    logger.debug(f"Params: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for marketplace listing items")
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/marketplace-listing-items"
    params = {
        "limit": limit,
        "offset": offset
    }
    if last_sync_date:
        params["last_sync_date"] = last_sync_date

    all_dataframes = []
    current_offset = offset
    total_records = 0

    while True:
        params["offset"] = current_offset
        logger.debug(f"Fetching batch: offset={current_offset}, limit={limit}")

        try:
            raw_data = api.get(endpoint, params=params)
            if not raw_data:
                logger.info("No more listing items data returned from Odoo API.")
                break

            # If the API returns a dict with "data", extract it. Otherwise assume raw_data is a list.
            if isinstance(raw_data, dict) and "data" in raw_data:
                raw_data = raw_data["data"]

            df_batch = pd.DataFrame(raw_data)
            batch_size = len(df_batch)
            total_records += batch_size
            logger.info(f"Fetched {batch_size} items (offset={current_offset})")

            if batch_size == 0:
                logger.info("Empty batch received; stopping pagination.")
                break

            all_dataframes.append(df_batch)
            if batch_size < limit:
                # Likely no more data left
                break

            current_offset += limit

        except Exception as e:
            logger.error(f"Error fetching listing items at offset {current_offset}: {e}")
            break

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Total marketplace listing items extracted: {len(final_df)}")
        return final_df
    else:
        logger.warning("No listing items extracted after pagination.")
        return pd.DataFrame()
