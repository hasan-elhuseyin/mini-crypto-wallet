"""blockchain-service initial schema

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE addresses (
            id         UUID         PRIMARY KEY,
            owner_ref  VARCHAR(128) NOT NULL UNIQUE,
            network    VARCHAR(32)  NOT NULL,
            address    VARCHAR(64)  NOT NULL UNIQUE,
            derivation VARCHAR(32)  NOT NULL DEFAULT 'random-keypair',
            status     VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
            created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
        );
        CREATE INDEX ix_addresses_address_lower ON addresses (lower(address));

        -- Encrypted private keys live alone, so no ordinary read path can join
        -- them into a response by accident.
        CREATE TABLE key_material (
            address               VARCHAR(64) PRIMARY KEY
                                  REFERENCES addresses(address) ON DELETE RESTRICT,
            encrypted_private_key BYTEA       NOT NULL,
            key_version           INTEGER     NOT NULL DEFAULT 1,
            created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE deposits (
            id            UUID          PRIMARY KEY,
            network       VARCHAR(32)   NOT NULL,
            asset         VARCHAR(16)   NOT NULL,
            tx_hash       VARCHAR(80)   NOT NULL,
            log_index     INTEGER       NOT NULL,
            to_address    VARCHAR(64)   NOT NULL,
            from_address  VARCHAR(64),
            amount        NUMERIC(78,0) NOT NULL,
            block_number  BIGINT        NOT NULL,
            block_hash    VARCHAR(80)   NOT NULL,
            confirmations INTEGER       NOT NULL DEFAULT 0,
            status        VARCHAR(16)   NOT NULL DEFAULT 'DETECTED',
            first_seen_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
            confirmed_at  TIMESTAMPTZ,
            reorged_at    TIMESTAMPTZ,
            -- The rule that makes deposit ingestion idempotent at the storage layer.
            CONSTRAINT uq_deposit_chain_identity UNIQUE (network, tx_hash, log_index),
            CONSTRAINT ck_deposit_amount_positive CHECK (amount > 0)
        );
        CREATE INDEX ix_deposits_status     ON deposits (status);
        CREATE INDEX ix_deposits_to_address ON deposits (to_address);

        CREATE TABLE outgoing_transactions (
            id              UUID          PRIMARY KEY,
            transfer_id     UUID          NOT NULL UNIQUE,
            network         VARCHAR(32)   NOT NULL,
            asset           VARCHAR(16)   NOT NULL,
            from_address    VARCHAR(64)   NOT NULL,
            to_address      VARCHAR(64)   NOT NULL,
            amount          NUMERIC(78,0) NOT NULL,
            tx_hash         VARCHAR(80)   UNIQUE,
            nonce           BIGINT,
            status          VARCHAR(16)   NOT NULL DEFAULT 'CREATED',
            block_number    BIGINT,
            block_hash      VARCHAR(80),
            confirmations   INTEGER       NOT NULL DEFAULT 0,
            failure_reason  TEXT,
            attempts        INTEGER       NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ,
            broadcast_at    TIMESTAMPTZ,
            correlation_id  VARCHAR(64),
            created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
            CONSTRAINT ck_outgoing_amount_positive CHECK (amount > 0)
        );
        CREATE INDEX ix_outgoing_status ON outgoing_transactions (status);

        CREATE TABLE scan_state (
            network            VARCHAR(32) PRIMARY KEY,
            last_scanned_block BIGINT      NOT NULL DEFAULT 0,
            last_scanned_hash  VARCHAR(80),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE outbox (
            id              BIGSERIAL   PRIMARY KEY,
            event_id        UUID        NOT NULL UNIQUE,
            event_type      VARCHAR(64) NOT NULL,
            envelope        JSONB       NOT NULL,
            correlation_id  VARCHAR(64),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at    TIMESTAMPTZ,
            attempts        INTEGER     NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ,
            last_error      TEXT
        );
        CREATE INDEX ix_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

        CREATE TABLE processed_events (
            consumer     VARCHAR(64) NOT NULL,
            event_id     UUID        NOT NULL,
            event_type   VARCHAR(64) NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer, event_id)
        );

        CREATE TABLE dead_letters (
            id             UUID        PRIMARY KEY,
            consumer       VARCHAR(64) NOT NULL,
            event_id       UUID        NOT NULL,
            event_type     VARCHAR(64) NOT NULL,
            envelope       JSONB       NOT NULL,
            delivery_count INTEGER     NOT NULL DEFAULT 0,
            error          TEXT        NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    # The simulated chain lives in its own schema: it models the *node*, not the
    # service. With CHAIN_BACKEND=web3 these tables are simply unused.
    op.execute(
        """
        CREATE SCHEMA IF NOT EXISTS mockchain;

        -- Keyed by hash: a reorg leaves two blocks at the same height, one of
        -- which is no longer canonical.
        CREATE TABLE mockchain.blocks (
            hash         VARCHAR(80) PRIMARY KEY,
            number       BIGINT      NOT NULL,
            parent_hash  VARCHAR(80) NOT NULL,
            is_canonical BOOLEAN     NOT NULL DEFAULT TRUE,
            mined_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX ix_mock_blocks_number ON mockchain.blocks (number);
        CREATE UNIQUE INDEX uq_mock_canonical_height
            ON mockchain.blocks (number) WHERE is_canonical;

        CREATE TABLE mockchain.transactions (
            hash         VARCHAR(80)   PRIMARY KEY,
            from_address VARCHAR(64)   NOT NULL,
            to_address   VARCHAR(64)   NOT NULL,
            amount       NUMERIC(78,0) NOT NULL,
            asset        VARCHAR(16)   NOT NULL DEFAULT 'USDT',
            status       VARCHAR(16)   NOT NULL DEFAULT 'PENDING',
            block_number BIGINT,
            block_hash   VARCHAR(80),
            log_index    INTEGER,
            is_mint      BOOLEAN       NOT NULL DEFAULT FALSE,
            fail_on_mine BOOLEAN       NOT NULL DEFAULT FALSE,
            created_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
        );
        CREATE INDEX ix_mock_tx_status ON mockchain.transactions (status);
        CREATE INDEX ix_mock_tx_block  ON mockchain.transactions (block_number);

        CREATE TABLE mockchain.token_balances (
            address VARCHAR(64)   PRIMARY KEY,
            amount  NUMERIC(78,0) NOT NULL DEFAULT 0
        );

        CREATE TABLE mockchain.faults (
            id                  INTEGER PRIMARY KEY,
            rpc_available       BOOLEAN NOT NULL DEFAULT TRUE,
            halt_mining         BOOLEAN NOT NULL DEFAULT FALSE,
            fail_next_transfers INTEGER NOT NULL DEFAULT 0
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP SCHEMA IF EXISTS mockchain CASCADE;
        DROP TABLE IF EXISTS dead_letters, processed_events, outbox, scan_state,
                             outgoing_transactions, deposits, key_material,
                             addresses CASCADE;
        """
    )
