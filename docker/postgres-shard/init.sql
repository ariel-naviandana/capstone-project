CREATE TABLE IF NOT EXISTS transactions (
    trx_id VARCHAR(50),
    account_no VARCHAR(50) NOT NULL,
    recipient_no VARCHAR(50),
    type VARCHAR(50) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    ref_no VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT tx_shard_pk PRIMARY KEY (trx_id, created_at)
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_no ON transactions(account_no);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_trx_id ON transactions(trx_id);
