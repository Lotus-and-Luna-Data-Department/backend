# flows/odoo_flows/listings/extract_listings.py
import logging
import pandas as pd
from typing import Optional
from prefect import task
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_listings(last_sync_date: Optional[str] = None,
                          limit: int = 100,
                          offset: int = 0) -> pd.DataFrame:
    """
    Extract marketplace listings from Odoo's /api/marketplace-listings endpoint,
    returning a pandas DataFrame.

    Paginates until no more records are returned.

    Args:
        last_sync_date (str, optional): Filter listings updated after this date (YYYY-MM-DD HH:MM:SS).
        limit (int, optional): Page size (default=100).
        offset (int, optional): Offset for pagination (default=0).

    Returns:
        pd.DataFrame: Listings data; empty DataFrame if none found.
    """
    logger.info("=== extract_odoo_listings: starting extraction ===")
    logger.debug(f"Params: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    # Initialize Odoo API wrapper
    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for marketplace listings")
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/marketplace-listings"
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
            # The API might return {"status": "success", "data": [...]}, or a plain list
            # Adjust if your Odoo endpoint wraps data differently
            if not raw_data:
                logger.info("No listings data returned from Odoo API.")
                break

            # If the endpoint returns {"data": [...]}, extract the actual list
            if isinstance(raw_data, dict) and "data" in raw_data:
                raw_data = raw_data["data"]

            df_batch = pd.DataFrame(raw_data)
            batch_size = len(df_batch)
            total_records += batch_size
            logger.info(f"Fetched batch of {batch_size} listings (offset={current_offset}).")

            if batch_size == 0:
                logger.info("Empty batch received; stopping pagination.")
                break

            all_dataframes.append(df_batch)

            # If we got fewer than 'limit', presumably no more data left
            if batch_size < limit:
                logger.info("Received fewer records than limit; stopping pagination.")
                break

            current_offset += limit

        except Exception as e:
            logger.error(f"Error fetching listings at offset {current_offset}: {e}")
            break

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Listings data extracted successfully: {len(final_df)} total.")
        return final_df
    else:
        logger.warning("No listings extracted after pagination.")
        return pd.DataFrame()
