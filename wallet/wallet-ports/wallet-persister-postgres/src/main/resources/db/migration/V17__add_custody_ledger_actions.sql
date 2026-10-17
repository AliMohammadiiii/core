CREATE TABLE IF NOT EXISTS custody_ledger_actions (
    operation_id VARCHAR(36) PRIMARY KEY,
    owner_uuid VARCHAR(36) NOT NULL,
    asset VARCHAR(25) NOT NULL,
    amount NUMERIC(78, 18) NOT NULL CHECK (amount > 0),
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('CREDIT', 'RESERVE', 'RELEASE', 'SETTLE')),
    reference_operation_id VARCHAR(36),
    payload_hash CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('PROCESSING', 'APPLIED')),
    transaction_id INTEGER REFERENCES transaction (id),
    terminal_operation_id VARCHAR(36) REFERENCES custody_ledger_actions (operation_id),
    terminal_kind VARCHAR(16) CHECK (terminal_kind IN ('RELEASE', 'SETTLE')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT custody_ledger_action_reference_shape CHECK (
        (kind IN ('CREDIT', 'RELEASE', 'SETTLE') AND reference_operation_id IS NOT NULL)
        OR (kind = 'RESERVE' AND reference_operation_id IS NULL)
    ),
    CONSTRAINT custody_ledger_action_status_shape CHECK (
        (status = 'PROCESSING' AND transaction_id IS NULL)
        OR (status = 'APPLIED' AND transaction_id IS NOT NULL)
    ),
    CONSTRAINT custody_ledger_action_terminal_shape CHECK (
        (terminal_operation_id IS NULL AND terminal_kind IS NULL)
        OR (kind = 'RESERVE' AND terminal_operation_id IS NOT NULL AND terminal_kind IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS custody_ledger_actions_owner_created_idx
    ON custody_ledger_actions (owner_uuid, created_at DESC, operation_id DESC);
