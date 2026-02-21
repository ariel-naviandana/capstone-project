-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) UNIQUE,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recipient_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    amount DOUBLE PRECISION NOT NULL CHECK (amount > 0),
    type VARCHAR(50) NOT NULL CHECK (type IN ('deposit', 'withdraw', 'transfer')),
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'success', 'failed')),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_recipient_id ON transactions(recipient_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);

-- Insert dummy users
INSERT INTO users (username, email, balance) VALUES
    ('user1', 'user1@example.com', 100000.0),
    ('user2', 'user2@example.com', 50000.0),
    ('user3', 'user3@example.com', 200000.0),
    ('user4', 'user4@example.com', 0.0),
    ('user5', 'user5@example.com', 150000.0)
ON CONFLICT (username) DO NOTHING;

-- Insert dummy transactions (CARA 1: INSERT satu per satu - PALING AMAN)
INSERT INTO transactions (user_id, recipient_id, amount, type, status, description)
SELECT id, NULL, 50000.0, 'deposit', 'success', 'Topup awal'
FROM users WHERE username = 'user1';

INSERT INTO transactions (user_id, recipient_id, amount, type, status, description)
SELECT id, NULL, 20000.0, 'withdraw', 'success', 'Tarik tunai'
FROM users WHERE username = 'user2';

INSERT INTO transactions (user_id, recipient_id, amount, type, status, description)
SELECT u1.id, u2.id, 30000.0, 'transfer', 'success', 'Transfer ke user1'
FROM users u1, users u2
WHERE u1.username = 'user3' AND u2.username = 'user1';

INSERT INTO transactions (user_id, recipient_id, amount, type, status, description)
SELECT id, NULL, 100000.0, 'deposit', 'pending', 'Topup besar'
FROM users WHERE username = 'user4';

INSERT INTO transactions (user_id, recipient_id, amount, type, status, description)
SELECT u1.id, u2.id, 50000.0, 'transfer', 'success', 'Transfer ke user2'
FROM users u1, users u2
WHERE u1.username = 'user5' AND u2.username = 'user2';