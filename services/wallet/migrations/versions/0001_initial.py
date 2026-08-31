"""wallet-service initial schema

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
        CREATE TABLE users (
            id          BIGSERIAL PRIMARY KEY,
            name        VARCHAR(128) NOT NULL,
            email       VARCHAR(255) NOT NULL UNIQUE,
            status      VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
            created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
        );

        CREATE TABLE wallets (
            id          UUID         PRIMARY KEY,
            user_id     BIGINT       NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            network     VARCHAR(32)  NOT NULL,
            asset       VARCHAR(16)  NOT NULL,
            address     VARCHAR(64)  NOT NULL UNIQUE,
            status      VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
            created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
            CONSTRAINT uq_wallet_user_network_asset UNIQUE (user_id, network, asset)
        );
        CREATE INDEX ix_wallets_user_id ON wallets (user_id);

        -- Derived snapshot of the ledger. `available = posted - reserved`.
        -- The CHECK constraints are the last line of defence against a negative
        -- balance: even a bug in the service cannot overdraw an account.
        CREATE TABLE balances (
            user_id     BIGINT        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            asset       VARCHAR(16)   NOT NULL,
            posted      NUMERIC(78,0) NOT NULL DEFAULT 0,
            reserved    NUMERIC(78,0) NOT NULL DEFAULT 0,
            version     INTEGER       NOT NULL DEFAULT 0,
            updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, asset),
            CONSTRAINT ck_balance_reserved_non_negative  CHECK (reserved >= 0),
            CONSTRAINT ck_balance_available_non_negative CHECK (posted - reserved >= 0)
        );

        -- Source of truth. Append-only; see the trigger below.
        CREATE TABLE ledger_entries (
            id              BIGSERIAL     PRIMARY KEY,
            entry_id        UUID          NOT NULL UNIQUE,
            user_id         BIGINT        NOT NULL REFERENCES users(id),
            asset           VARCHAR(16)   NOT NULL,
            amount          NUMERIC(78,0) NOT NULL,
            entry_type      VARCHAR(24)   NOT NULL,
            reference_type  VARCHAR(24)   NOT NULL,
            reference_id    VARCHAR(200)  NOT NULL,
            correlation_id  VARCHAR(64),
            created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
            CONSTRAINT uq_ledger_idempotency
                UNIQUE (user_id, entry_type, reference_type, reference_id),
            CONSTRAINT ck_ledger_amount_non_zero CHECK (amount <> 0)
        );
        CREATE INDEX ix_ledger_entries_user_id  ON ledger_entries (user_id);
        CREATE INDEX ix_ledger_user_created     ON ledger_entries (user_id, created_at);

        CREATE TABLE transfers (
            id              UUID          PRIMARY KEY,
            idempotency_key VARCHAR(128)  NOT NULL UNIQUE,
            from_user_id    BIGINT        NOT NULL REFERENCES users(id),
            to_user_id      BIGINT        NOT NULL REFERENCES users(id),
            asset           VARCHAR(16)   NOT NULL,
            network         VARCHAR(32)   NOT NULL,
            amount          NUMERIC(78,0) NOT NULL,
            from_address    VARCHAR(64)   NOT NULL,
            to_address      VARCHAR(64)   NOT NULL,
            status          VARCHAR(16)   NOT NULL DEFAULT 'CREATED',
            tx_hash         VARCHAR(80),
            failure_code    VARCHAR(48),
            failure_reason  TEXT,
            correlation_id  VARCHAR(64),
            created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
            settled_at      TIMESTAMPTZ,
            CONSTRAINT ck_transfer_amount_positive    CHECK (amount > 0),
            CONSTRAINT ck_transfer_distinct_parties   CHECK (from_user_id <> to_user_id)
        );
        CREATE INDEX ix_transfers_status       ON transfers (status);
        CREATE INDEX ix_transfers_from_user_id ON transfers (from_user_id);
        CREATE INDEX ix_transfers_to_user_id   ON transfers (to_user_id);

        -- API idempotency: claimed and completed in the same transaction as the
        -- transfer it protects.
        CREATE TABLE idempotency_keys (
            scope           VARCHAR(64)  NOT NULL,
            key             VARCHAR(128) NOT NULL,
            request_hash    VARCHAR(64)  NOT NULL,
            response_status INTEGER      NOT NULL,
            response_body   JSONB        NOT NULL,
            resource_id     VARCHAR(64),
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
            PRIMARY KEY (scope, key)
        );

        -- Transactional outbox: written with the business change, published later.
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

        -- Consumer-side deduplication, written in the handler's transaction.
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
    # Financial records are corrected with new entries, never edited or removed.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION ledger_entries_append_only() RETURNS trigger AS $fn$
        BEGIN
            RAISE EXCEPTION
                'ledger_entries is append-only (attempted %). Post a REVERSAL entry instead.',
                TG_OP;
        END;
        $fn$ LANGUAGE plpgsql;

        CREATE TRIGGER trg_ledger_entries_append_only
            BEFORE UPDATE OR DELETE ON ledger_entries
            FOR EACH ROW EXECUTE FUNCTION ledger_entries_append_only();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_ledger_entries_append_only ON ledger_entries;
        DROP FUNCTION IF EXISTS ledger_entries_append_only();
        DROP TABLE IF EXISTS dead_letters, processed_events, outbox, idempotency_keys,
                             transfers, ledger_entries, balances, wallets, users CASCADE;
        """
    )
