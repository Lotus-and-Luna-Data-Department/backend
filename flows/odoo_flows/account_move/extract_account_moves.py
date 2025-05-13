# flows/odoo_flows/account_moves/extract_account_moves.py
import logging
import pandas as pd
from typing import Optional
from prefect import task
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_account_moves(last_sync_date: Optional[str] = None,
                                limit: int = 100,
                                offset: int = 0) -> pd.DataFrame:
    logger.info("=== extract_odoo_account_moves: starting extraction ===")
    logger.debug(f"Params: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for account moves")
    except Exception as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/account_moves"
    params = {"limit": limit, "offset": offset}
    if last_sync_date:
        params["start_date"] = last_sync_date

    all_batches = []
    current_offset = offset  

    while True:
        params["offset"] = current_offset
        try:
            response = api.get(endpoint, params=params)
            if isinstance(response, dict) and "data" in response:
                response = response["data"]
            if not response:
                break

            batch = pd.DataFrame(response)
            all_batches.append(batch)
            if len(batch) < limit:
                break
            current_offset += limit  
        except Exception as e:
            logger.error(f"Error fetching account moves at offset={current_offset}: {e}")
            break

    return pd.concat(all_batches, ignore_index=True) if all_batches else pd.DataFrame()
