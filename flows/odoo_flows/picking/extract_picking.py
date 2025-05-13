# flows/odoo_flows/stock_pickings/extract_stock_pickings.py
import logging
import pandas as pd
from typing import Optional
from prefect import task
from flows.odoo_flows.odoo_utils import OdooAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def extract_odoo_stock_pickings(last_sync_date: Optional[str] = None,
                                 limit: int = 100,
                                 offset: int = 0) -> pd.DataFrame:
    logger.info("=== extract_odoo_stock_pickings: starting extraction ===")
    logger.debug(f"Params: last_sync_date={last_sync_date}, limit={limit}, offset={offset}")

    try:
        api = OdooAPI()
        logger.info("OdooAPI initialized for stock pickings")
    except Exception as e:
        logger.error(f"Failed to initialize OdooAPI: {e}")
        return pd.DataFrame()

    endpoint = "api/stock_pickings"
    params = {"limit": limit, "offset": offset}
    if last_sync_date:
        params["start_date"] = last_sync_date

    all_batches = []
    current_offset = offset

    while True:
        params["offset"] = current_offset
        try:
            raw_data = api.get(endpoint, params=params)
            if isinstance(raw_data, dict) and "data" in raw_data:
                raw_data = raw_data["data"]
            if not raw_data:
                break

            batch = pd.DataFrame(raw_data)
            all_batches.append(batch)
            if len(batch) < limit:
                break
            current_offset += limit
        except Exception as e:
            logger.error(f"Error fetching stock pickings at offset={current_offset}: {e}")
            break

    return pd.concat(all_batches, ignore_index=True) if all_batches else pd.DataFrame()
