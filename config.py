from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import logging
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Settings(BaseSettings):
    ENV: str = Field(default="production")  # 'production' or 'test'

    PREFECT_DB_PASSWORD: str  
    
    # Database: both test and prod
    DB_HOST_TEST: str
    DB_PORT_TEST: int
    DB_USER_TEST: str
    DB_PASSWORD_TEST: str
    DB_NAME_TEST: str

    DB_HOST_PROD: str
    DB_PORT_PROD: int
    DB_USER_PROD: str
    DB_PASSWORD_PROD: str
    DB_NAME_PROD: str

    # Odoo: test vs. prod
    ODOO_URL_TEST: str
    ODOO_DB_TEST: str
    ODOO_USER_TEST: str
    ODOO_PASSWORD_TEST: str

    ODOO_URL_PROD: str
    ODOO_DB_PROD: str
    ODOO_USER_PROD: str
    ODOO_PASSWORD_PROD: str

    # Shopify: test vs. prod
    SHOP_URL_TEST: str
    SHOP_TOKEN_TEST: str
    SHOP_KEY_TEST: str
    SHOP_SECRET_KEY_TEST: str

    SHOP_URL_PROD: str
    SHOP_TOKEN_PROD: str
    SHOP_KEY_PROD: str
    SHOP_SECRET_KEY_PROD: str

    # Defaults / Logging
    DEFAULT_DATE_TEST: str
    DEFAULT_DATE_PROD: str
    LOG_LEVEL: str = Field(default="INFO")

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"  # Allow extra environment variables without crashing
    )

    @property
    def is_test(self) -> bool:
        return self.ENV.lower() == "test"

    @property
    def DB_HOST(self):
        return self.DB_HOST_TEST if self.is_test else self.DB_HOST_PROD

    @property
    def DB_PORT(self):
        return self.DB_PORT_TEST if self.is_test else self.DB_PORT_PROD

    @property
    def DB_USER(self):
        return self.DB_USER_TEST if self.is_test else self.DB_USER_PROD

    @property
    def DB_PASSWORD(self):
        return self.DB_PASSWORD_TEST if self.is_test else self.DB_PASSWORD_PROD

    @property
    def DB_NAME(self):
        return self.DB_NAME_TEST if self.is_test else self.DB_NAME_PROD

    @property
    def ODOO_URL(self):
        return self.ODOO_URL_TEST if self.is_test else self.ODOO_URL_PROD

    @property
    def ODOO_DB(self):
        return self.ODOO_DB_TEST if self.is_test else self.ODOO_DB_PROD

    @property
    def ODOO_USER(self):
        return self.ODOO_USER_TEST if self.is_test else self.ODOO_USER_PROD

    @property
    def ODOO_PASSWORD(self):
        return self.ODOO_PASSWORD_TEST if self.is_test else self.ODOO_PASSWORD_PROD

    @property
    def SHOP_URL(self):
        return self.SHOP_URL_TEST if self.is_test else self.SHOP_URL_PROD

    @property
    def SHOP_TOKEN(self):
        return self.SHOP_TOKEN_TEST if self.is_test else self.SHOP_TOKEN_PROD

    @property
    def SHOP_KEY(self):
        return self.SHOP_KEY_TEST if self.is_test else self.SHOP_KEY_PROD

    @property
    def SHOP_SECRET_KEY(self):
        return self.SHOP_SECRET_KEY_TEST if self.is_test else self.SHOP_SECRET_KEY_PROD

    @property
    def DEFAULT_DATE(self):
        return self.DEFAULT_DATE_TEST if self.is_test else self.DEFAULT_DATE_PROD


# Final settings instance
settings = Settings()

logger.debug(
    f"Loaded environment: {settings.ENV} | DB={settings.DB_NAME} | Odoo={settings.ODOO_DB} | Shopify={settings.SHOP_URL}"
)
