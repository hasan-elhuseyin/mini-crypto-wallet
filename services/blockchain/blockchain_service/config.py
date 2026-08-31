from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_prefix="")

    service_name: str = "blockchain-service"
    version: str = "1.0.0"
    log_level: str = "INFO"
    log_json: bool = True

    database_url: str = Field(
        default="postgresql+psycopg://mcw:mcw@localhost:5432/blockchain",
        validation_alias="BLOCKCHAIN_DATABASE_URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", validation_alias="REDIS_URL")

    auth_enabled: bool = Field(default=True, validation_alias="AUTH_ENABLED")
    api_key: str = Field(default="dev-api-key", validation_alias="API_KEY")
    internal_api_key: str = Field(default="dev-internal-key", validation_alias="INTERNAL_API_KEY")

    # --- custody ---------------------------------------------------------
    #: Fernet key used to encrypt private keys at rest. In production this is a
    #: KMS data key, not an env var (see README "Security Considerations").
    keystore_encryption_key: str = Field(
        default="", validation_alias="KEYSTORE_ENCRYPTION_KEY"
    )

    # --- chain -----------------------------------------------------------
    #: "mock" (default, self-contained simulated chain) or "web3" (anvil/testnet).
    chain_backend: str = Field(default="mock", validation_alias="CHAIN_BACKEND")
    network: str = Field(default="BSC", validation_alias="CHAIN_NETWORK")
    confirmations_required: int = Field(default=3, validation_alias="CONFIRMATIONS_REQUIRED")
    finality_depth: int = Field(default=15, validation_alias="FINALITY_DEPTH")
    scan_batch_blocks: int = Field(default=200, validation_alias="SCAN_BATCH_BLOCKS")
    reorg_safety_blocks: int = Field(default=20, validation_alias="REORG_SAFETY_BLOCKS")

    mock_block_time_seconds: float = Field(default=1.0, validation_alias="MOCK_BLOCK_TIME_SECONDS")
    mock_auto_mine: bool = Field(default=True, validation_alias="MOCK_AUTO_MINE")

    web3_rpc_url: str = Field(default="http://localhost:8545", validation_alias="WEB3_RPC_URL")
    web3_chain_id: int = Field(default=31337, validation_alias="WEB3_CHAIN_ID")
    usdt_contract_address: str = Field(
        default="0x55d398326f99059fF775485246999027B3197955",
        validation_alias="USDT_CONTRACT_ADDRESS",
    )
    #: Account that funds gas / acts as the faucet on a local chain.
    treasury_private_key: str = Field(default="", validation_alias="TREASURY_PRIVATE_KEY")

    # --- workers ---------------------------------------------------------
    broadcast_max_attempts: int = Field(default=5, validation_alias="BROADCAST_MAX_ATTEMPTS")
    pending_timeout_seconds: int = Field(default=120, validation_alias="PENDING_TIMEOUT_SECONDS")
    consumer_group: str = "blockchain-service"
    consumer_name: str = Field(default="blockchain-1", validation_alias="CONSUMER_NAME")
    consumer_max_delivery: int = Field(default=5, validation_alias="CONSUMER_MAX_DELIVERY")
    consumer_claim_idle_ms: int = Field(default=15_000, validation_alias="CONSUMER_CLAIM_IDLE_MS")
    worker_poll_seconds: float = Field(default=1.0, validation_alias="WORKER_POLL_SECONDS")

    #: Guard rail: simulation endpoints must never be reachable in production.
    enable_simulation_api: bool = Field(default=True, validation_alias="ENABLE_SIMULATION_API")


@lru_cache
def get_settings() -> Settings:
    return Settings()
