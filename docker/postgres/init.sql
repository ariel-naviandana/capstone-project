-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) UNIQUE,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create transactions table (Diubah menjadi Partition Table)
-- SHARDING SIMULATION:
-- Jika data membesar, kita melakukan logis sharding by user_id di level aplikasi.
-- Misal: UserID Ganjil masuk ke Shard A, UserID Genap masuk ke Shard B.
-- Secara fisik, kita akan punya 2 cluster DB postgres yang identik strukturnya seperti ini.
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL,
    tx_id VARCHAR(36),
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recipient_id BIGINT,
    amount DOUBLE PRECISION NOT NULL CHECK (amount > 0),
    type VARCHAR(50) NOT NULL CHECK (type IN ('deposit', 'withdraw', 'transfer')),
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'success', 'failed')),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT tx_pk PRIMARY KEY (id, created_at),
    CONSTRAINT tx_id_unique UNIQUE (tx_id, created_at)
) PARTITION BY RANGE (created_at);

-- Create initial partitions for 2026 (April to December)
CREATE TABLE transactions_y2026m04 PARTITION OF transactions FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE transactions_y2026m05 PARTITION OF transactions FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE transactions_y2026m06 PARTITION OF transactions FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE transactions_y2026m07 PARTITION OF transactions FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE transactions_y2026m08 PARTITION OF transactions FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE transactions_y2026m09 PARTITION OF transactions FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE transactions_y2026m10 PARTITION OF transactions FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE transactions_y2026m11 PARTITION OF transactions FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE transactions_y2026m12 PARTITION OF transactions FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_recipient_id ON transactions(recipient_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_tx_id ON transactions(tx_id);

-- Insert 100000 dummy users
INSERT INTO users (username, email, balance)
SELECT 
    'user' || i, 
    'user' || i || '@example.com', 
    100000.0
FROM generate_series(1, 100000) AS i
ON CONFLICT (username) DO NOTHING;

-- Insert dummy transactions (Hanya untuk bulan april agar masuk ke partisi y2026m04)
INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT gen_random_uuid()::varchar, id, NULL, 50000.0, 'deposit', 'success', 'Topup awal', '2026-04-05 10:00:00', '2026-04-05 10:00:00'
FROM users WHERE username = 'user1';

INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT gen_random_uuid()::varchar, id, NULL, 20000.0, 'withdraw', 'success', 'Tarik tunai', '2026-04-05 11:00:00', '2026-04-05 11:00:00'
FROM users WHERE username = 'user2';

INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT gen_random_uuid()::varchar, u1.id, u2.id, 30000.0, 'transfer', 'success', 'Transfer ke user1', '2026-04-06 12:00:00', '2026-04-06 12:00:00'
FROM users u1, users u2
WHERE u1.username = 'user3' AND u2.username = 'user1';

INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT gen_random_uuid()::varchar, id, NULL, 100000.0, 'deposit', 'pending', 'Topup besar', '2026-04-06 13:00:00', '2026-04-06 13:00:00'
FROM users WHERE username = 'user4';

INSERT INTO transactions (tx_id, user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT gen_random_uuid()::varchar, u1.id, u2.id, 50000.0, 'transfer', 'success', 'Transfer ke user2', '2026-04-07 14:00:00', '2026-04-07 14:00:00'
FROM users u1, users u2
WHERE u1.username = 'user5' AND u2.username = 'user2';