# File: flows/odoo_flows/account_payments/extract_account_payments.py

import logging
import pandas as pd
from typing import Optional
from prefect import task
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_account_payments(last_sync_date: Optional[str] = None,
                                  limit: int = 100,
                                  offset: int = 0) -> pd.DataFrame:
    """
    Extract account.payment records from Odoo API, returning a pandas DataFrame.
    Implements pagination to fetch all available records by incrementing offset.

    Args:
        last_sync_date (str, optional): Filter payments payment_date >= this date
            (YYYY-MM-DD HH:MM:SS). If None, fetch all.
        limit (int): Number of records per batch.
        offset (int): Starting offset for pagination.

    Returns:
        pd.DataFrame: DataFrame of all account.payment records. Could be empty.
    """
    logger.info("=== extract_odoo_account_payments: starting extraction ===")
    logger.debug(f"Parameters: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    # Initialize Odoo API client
    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for account payments")
    except Exception as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/account_payments"
    params = {"limit": limit, "offset": offset}
    if last_sync_date:
        params["start_date"] = last_sync_date

    all_batches = []
    current_offset = offset

    while True:
        params["offset"] = current_offset
        try:
            response = api.get(endpoint, params=params)
            # If the API wraps data in a dict
            if isinstance(response, dict) and "data" in response:
                response = response["data"]
            if not response:
                logger.info("No more account payment data returned from API.")
                break

            batch = pd.DataFrame(response)
            batch_size = len(batch)
            logger.info(f"Fetched account payments batch: {batch_size} records (offset={current_offset})")
            all_batches.append(batch)

            if batch_size < limit:
                logger.info("Batch size < limit; ending pagination.")
                break
            current_offset += limit

        except Exception as e:
            logger.error(f"Error fetching account payments at offset={current_offset}: {e}")
            break

    if all_batches:
        final_df = pd.concat(all_batches, ignore_index=True)
        logger.info(f"Account payments extracted successfully: {len(final_df)} total records")
        return final_df
    else:
        logger.warning("No account payments extracted after pagination.")
        return pd.DataFrame()
