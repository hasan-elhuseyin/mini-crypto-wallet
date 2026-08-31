from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_prefix="")

    service_name: str = "wallet-service"
    version: str = "1.0.0"
    log_level: str = "INFO"
    log_json: bool = True

    database_url: str = Field(
        default="postgresql+psycopg://mcw:mcw@localhost:5432/wallet",
        validation_alias="WALLET_DATABASE_URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", validation_alias="REDIS_URL")

    auth_enabled: bool = Field(default=True, validation_alias="AUTH_ENABLED")
    api_key: str = Field(default="dev-api-key", validation_alias="API_KEY")
    internal_api_key: str = Field(default="dev-internal-key", validation_alias="INTERNAL_API_KEY")

    blockchain_service_url: str = Field(
        default="http://localhost:8001", validation_alias="BLOCKCHAIN_SERVICE_URL"
    )
    blockchain_timeout_seconds: float = Field(
        default=5.0, validation_alias="BLOCKCHAIN_TIMEOUT_SECONDS"
    )

    default_network: str = Field(default="BSC", validation_alias="CHAIN_NETWORK")
    default_asset: str = "USDT"

    consumer_group: str = "wallet-service"
    consumer_name: str = Field(default="wallet-1", validation_alias="CONSUMER_NAME")
    consumer_max_delivery: int = Field(default=5, validation_alias="CONSUMER_MAX_DELIVERY")
    consumer_claim_idle_ms: int = Field(default=15_000, validation_alias="CONSUMER_CLAIM_IDLE_MS")
    worker_poll_seconds: float = Field(default=1.0, validation_alias="WORKER_POLL_SECONDS")

    #: Statement timeout for the balance row lock; keeps a hot account from
    #: queueing requests indefinitely.
    lock_timeout_ms: int = Field(default=3_000, validation_alias="LOCK_TIMEOUT_MS")


@lru_cache
def get_settings() -> Settings:
    return Settings()
