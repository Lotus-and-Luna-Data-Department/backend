# flows/odoo_flows/listing_items/transform_listing_items.py
import logging
import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def transform_odoo_listing_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw listing items DataFrame into a standardized schema.

    Final columns:
        - item_id
        - continue_selling
        - default_code
        - is_listed
        - listing
        - marketplace_id
        - sale_price
        - created_on
        - last_updated
    """
    logger.info("=== transform_odoo_listing_items: starting transform ===")

    expected_columns = [
        "item_id",
        "continue_selling",
        "default_code",
        "is_listed",
        "listing",
        "marketplace_id",
        "sale_price",
        "created_on",
        "last_updated"
    ]

    if df.empty:
        logger.warning("No listing items found; returning empty DataFrame with the expected schema.")
        return pd.DataFrame(columns=expected_columns)

    # Map fields from your endpoint's JSON keys to a standardized set
    df_transformed = pd.DataFrame({
        "item_id": df["id"],
        "continue_selling": df["continue_selling"],
        "default_code": df["default_code"],
        "is_listed": df["is_listed"],
        "listing": df["listing"],
        "marketplace_id": df["marketplace_id"],
        "sale_price": df["sale_price"],
        "created_on": df["created_on"],
        "last_updated": df["last_updated"],
    })

    # Convert date fields
    for col in ["created_on", "last_updated"]:
        df_transformed[col] = pd.to_datetime(df_transformed[col], errors="coerce")
        df_transformed[col] = df_transformed[col].where(df_transformed[col].notna(), None)

    # Convert booleans safely (some endpoints return "true"/"false" strings)
    df_transformed["continue_selling"] = df_transformed["continue_selling"].apply(
        lambda x: bool(x) if pd.notnull(x) else None
    )
    df_transformed["is_listed"] = df_transformed["is_listed"].apply(
        lambda x: bool(x) if pd.notnull(x) else None
    )

    # Ensure sale_price is numeric
    df_transformed["sale_price"] = pd.to_numeric(df_transformed["sale_price"], errors="coerce")

    # Ensure string columns are cast to str (null-safe)
    string_cols = ["item_id", "default_code", "listing", "marketplace_id"]
    for col in string_cols:
        df_transformed[col] = df_transformed[col].astype(str).where(df_transformed[col].notna(), None)

    logger.info(f"transform_odoo_listing_items: final shape={df_transformed.shape}")
    return df_transformed
