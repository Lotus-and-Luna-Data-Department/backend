from sqlalchemy import create_engine, text
import logging
import pandas as pd
from config import settings  # Import centralized settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.
    Uses the production database parameters when ENV is "production" (via the settings properties).
    """

    # Retrieve the connection parameters from settings.
    db_user = settings.DB_USER
    db_password = settings.DB_PASSWORD
    db_host = settings.DB_HOST
    db_port = settings.DB_PORT
    db_name = settings.DB_NAME  # picks production or test based on ENV

    # Add ?sslmode=require to enforce SSL
    connection_string = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    )

    logger.debug(f"Creating engine with: {connection_string}")
    engine = create_engine(connection_string)
    return engine
