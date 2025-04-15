# shop_utils.py
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
import shopify
import time
import json
from config import settings  # Use centralized settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ShopifyClient:
    def __init__(self):
        # Fetch Shopify credentials from centralized settings
        self.shop_url = settings.SHOP_URL
        self.token = settings.SHOP_TOKEN  # Use as the private_app_password
        if not all([self.shop_url, self.token]):
            raise ValueError("Missing required Shopify credentials in configuration")
        self.api_version = "2025-01"  # Updated to the latest API version

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logger, logging.INFO)
    )
    def fetch(self, endpoint, params=None):
        """Fetch raw Shopify REST API data with basic rate limiting and error handling."""
        url = f"https://{self.shop_url}/admin/api/{self.api_version}/{endpoint}.json"
        try:
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=30)
            response.raise_for_status()
            call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40').split('/')
            calls_made = int(call_limit[0])
            calls_max = int(call_limit[1])
            if calls_made > calls_max * 0.8:
                logger.warning(f"Approaching REST rate limit: {calls_made}/{calls_max}, delaying 2 seconds")
                time.sleep(2)
            logger.debug(f"Fetched raw REST data for {endpoint} with params: {params}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch REST {endpoint}: {e}")
            raise

    def get_headers(self):
        """Return headers with the required Shopify access token for REST requests."""
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.token
        }

    def execute(self, query, variables=None):
        """Execute a Shopify GraphQL query with session activation and return the parsed response."""
        try:
            session = shopify.Session(self.shop_url, self.api_version, self.token)
            shopify.ShopifyResource.activate_session(session)
            response = shopify.GraphQL().execute(query, variables or {})
            shopify.ShopifyResource.clear_session()

            logger.debug(f"Executed GraphQL query: {query[:50]}... with variables: {variables}")
            parsed = response if isinstance(response, dict) else json.loads(response)
            if 'errors' in parsed:
                logger.error(f"GraphQL errors: {parsed['errors']}")
                if any(error['message'].startswith("Field 'returns'") for error in parsed.get('errors', [])):
                    logger.critical("The 'returns' field is not supported. Check API version or store permissions.")
            return json.dumps(parsed)  # Return as string to maintain current behavior
        except Exception as e:
            logger.error(f"Failed to execute GraphQL query: {e}")
            raise

def safe_get(data, *keys, default="0"):
    """Safely extract nested values from a dictionary, returning a default if any key is missing."""
    current = data
    for key in keys:
        try:
            if current is None or key not in current:
                return default
            current = current[key]
        except (TypeError, KeyError, AttributeError):
            return default
    return current if current is not None else default
