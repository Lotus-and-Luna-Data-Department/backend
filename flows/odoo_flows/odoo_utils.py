# odoo_utils.py
import requests
from config import settings  # Use centralized settings

class OdooAPI:
    def __init__(self):
        # Fetch credentials from the centralized configuration
        self.base_url = settings.ODOO_URL
        self.db = settings.ODOO_DB
        self.username = settings.ODOO_USER
        self.password = settings.ODOO_PASSWORD

        # Validate credentials
        if not all([self.base_url, self.db, self.username, self.password]):
            raise ValueError("Missing required Odoo credentials in configuration")
        
        self.session = requests.Session()
        self.uid = None
        self._authenticate()

    def _authenticate(self):
        url = f"{self.base_url}/web/session/authenticate"
        payload = {
            "jsonrpc": "2.0",
            "params": {
                "db": self.db,
                "login": self.username,
                "password": self.password
            }
        }
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        result = response.json().get("result")
        if not result or not result.get("uid"):
            raise ValueError("Authentication failed")
        self.uid = result["uid"]
        print(f"Authenticated! User ID: {self.uid}")

    def get(self, endpoint, params=None):
        """
        Make a GET request to the Odoo API endpoint with optional query parameters.
        Returns the data portion of the API response.
        """
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(
            url,
            headers={"Content-Type": "application/json"},
            params=params
        )
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "success":
            raise ValueError(f"API Error: {data.get('message')}")
        return data["data"]
