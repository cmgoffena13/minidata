from functools import lru_cache
from typing import Literal

from pydantic import HttpUrl, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class BaseConfig(BaseSettings):
    ENV_STATE: str | None = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Store all environment variables that can be accessed globally
class GlobalConfig(BaseConfig):
    LOG_LEVEL: LogLevel = "INFO"
    SERVICE_NAME: str = "minidata"
    OPEN_TELEMETRY_TRACE_ENDPOINT: HttpUrl | None = None
    OPEN_TELEMETRY_LOG_ENDPOINT: HttpUrl | None = None
    OPEN_TELEMETRY_AUTHORIZATION_TOKEN: str | None = None
    OPEN_TELEMETRY_FLAG: bool = False

    @model_validator(mode="after")
    def require_otel_config(self):
        if self.OPEN_TELEMETRY_FLAG and (
            self.OPEN_TELEMETRY_TRACE_ENDPOINT is None
            or self.OPEN_TELEMETRY_LOG_ENDPOINT is None
            or self.OPEN_TELEMETRY_AUTHORIZATION_TOKEN is None
        ):
            raise ValueError(
                "OPEN_TELEMETRY_FLAG is enabled; "
                "OPEN_TELEMETRY_TRACE_ENDPOINT, OPEN_TELEMETRY_LOG_ENDPOINT, "
                "and OPEN_TELEMETRY_AUTHORIZATION_TOKEN must be set"
            )
        return self


class DevConfig(GlobalConfig):
    LOG_LEVEL: LogLevel = "DEBUG"  # Overrides the global LOG_LEVEL

    model_config = SettingsConfigDict(env_prefix="DEV_")


class TestConfig(GlobalConfig):
    LOG_LEVEL: LogLevel = "DEBUG"

    model_config = SettingsConfigDict(env_prefix="TEST_")


class ProdConfig(GlobalConfig):
    LOG_LEVEL: LogLevel = "WARNING"
    OPEN_TELEMETRY_FLAG: bool = True

    model_config = SettingsConfigDict(env_prefix="PROD_")


@lru_cache
def get_config(env_state: str) -> GlobalConfig:
    if not env_state:
        raise ValueError("ENV_STATE is not set. Possible values are: DEV, TEST, PROD")
    env_state = env_state.lower()

    configs = {"dev": DevConfig, "prod": ProdConfig, "test": TestConfig}
    return configs[env_state]()


config = get_config(BaseConfig().ENV_STATE)
