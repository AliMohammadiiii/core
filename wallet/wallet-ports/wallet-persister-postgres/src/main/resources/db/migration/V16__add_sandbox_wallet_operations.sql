CREATE TABLE sandbox_wallet_operations (
    id VARCHAR(36) PRIMARY KEY,
    owner_uuid VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    type VARCHAR(16) NOT NULL,
    currency VARCHAR(64) NOT NULL,
    amount NUMERIC(36, 18) NOT NULL CHECK (amount > 0),
    status VARCHAR(16) NOT NULL,
    ledger_transaction_id VARCHAR(255) NOT NULL,
    withdrawal_uuid VARCHAR(36),
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT sandbox_wallet_operations_owner_key_unique UNIQUE (owner_uuid, idempotency_key)
);

CREATE INDEX sandbox_wallet_operations_owner_created_idx
    ON sandbox_wallet_operations (owner_uuid, created_at DESC, id DESC);
