-- 1. Customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2. Customer Identities
CREATE TABLE IF NOT EXISTS customer_identities (
    customer_id VARCHAR(50) PRIMARY KEY REFERENCES customers(customer_id) ON DELETE CASCADE,
    nik VARCHAR(50) UNIQUE,
    ktp_photo_url VARCHAR(255),
    selfie_url VARCHAR(255),
    kyc_status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 3. Customer Contacts
CREATE TABLE IF NOT EXISTS customer_contacts (
    customer_id VARCHAR(50) PRIMARY KEY REFERENCES customers(customer_id) ON DELETE CASCADE,
    phone VARCHAR(50) UNIQUE,
    email VARCHAR(255) UNIQUE,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 4. Customer Auths
CREATE TABLE IF NOT EXISTS customer_auths (
    customer_id VARCHAR(50) PRIMARY KEY REFERENCES customers(customer_id) ON DELETE CASCADE,
    pin_hash VARCHAR(255),
    biometric_enabled BOOLEAN DEFAULT FALSE,
    device_id VARCHAR(255),
    mpin_attempts INT DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 5. Customer Preferences
CREATE TABLE IF NOT EXISTS customer_preferences (
    customer_id VARCHAR(50) PRIMARY KEY REFERENCES customers(customer_id) ON DELETE CASCADE,
    language VARCHAR(10) DEFAULT 'id',
    notif_enabled BOOLEAN DEFAULT TRUE,
    dark_mode BOOLEAN DEFAULT FALSE,
    default_account VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 6. Accounts
CREATE TABLE IF NOT EXISTS accounts (
    account_no VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL DEFAULT 'savings',
    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    currency VARCHAR(10) NOT NULL DEFAULT 'IDR',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Note: In PostgreSQL, adding an FK where the referenced table doesn't exist yet will fail, 
-- so we alter preferences here, or we just don't enforce default_account FK to keep it simple.
ALTER TABLE customer_preferences ADD CONSTRAINT fk_default_account FOREIGN KEY (default_account) REFERENCES accounts(account_no) ON DELETE SET NULL;

-- 7. Transactions (Partitioned)
CREATE TABLE IF NOT EXISTS transactions (
    trx_id VARCHAR(50),
    account_no VARCHAR(50) NOT NULL REFERENCES accounts(account_no) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    ref_no VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT tx_pk PRIMARY KEY (trx_id, created_at)
) PARTITION BY RANGE (created_at);

-- Partitions for Transactions
CREATE TABLE transactions_y2026m04 PARTITION OF transactions FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE transactions_y2026m05 PARTITION OF transactions FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE transactions_y2026m06 PARTITION OF transactions FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE transactions_y2026m07 PARTITION OF transactions FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE transactions_y2026m08 PARTITION OF transactions FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE transactions_y2026m09 PARTITION OF transactions FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE transactions_y2026m10 PARTITION OF transactions FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE transactions_y2026m11 PARTITION OF transactions FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE transactions_y2026m12 PARTITION OF transactions FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- 8. Cards
CREATE TABLE IF NOT EXISTS cards (
    card_no_masked VARCHAR(50) PRIMARY KEY,
    account_no VARCHAR(50) NOT NULL REFERENCES accounts(account_no) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    expiry VARCHAR(10),
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    "limit" DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 9. Notifications
CREATE TABLE IF NOT EXISTS notifications (
    notif_id VARCHAR(50) PRIMARY KEY,
    account_no VARCHAR(50) NOT NULL REFERENCES accounts(account_no) ON DELETE CASCADE,
    channel VARCHAR(50),
    title VARCHAR(255),
    read BOOLEAN DEFAULT FALSE,
    trx_ref VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_customers_status ON customers(status);
CREATE INDEX IF NOT EXISTS idx_customer_identities_nik ON customer_identities(nik);
CREATE INDEX IF NOT EXISTS idx_customer_contacts_phone ON customer_contacts(phone);
CREATE INDEX IF NOT EXISTS idx_customer_contacts_email ON customer_contacts(email);
CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account_no ON transactions(account_no);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_cards_account_no ON cards(account_no);
CREATE INDEX IF NOT EXISTS idx_notifications_account_no ON notifications(account_no);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at);

-- 10. Dummy Data Generation for 100k customers (using generate_series for performance)

-- Customers
INSERT INTO customers (customer_id, full_name, status)
SELECT 
    'CUS-' || LPAD(i::text, 8, '0'),
    'User ' || i,
    'active'
FROM generate_series(1, 100000) AS i
ON CONFLICT (customer_id) DO NOTHING;

-- Identities
INSERT INTO customer_identities (customer_id, nik, kyc_status)
SELECT 
    'CUS-' || LPAD(i::text, 8, '0'),
    '3271' || LPAD(i::text, 12, '0'),
    'verified'
FROM generate_series(1, 100000) AS i
ON CONFLICT (customer_id) DO NOTHING;

-- Contacts
INSERT INTO customer_contacts (customer_id, phone, email)
SELECT 
    'CUS-' || LPAD(i::text, 8, '0'),
    '+62812' || LPAD(i::text, 7, '0'),
    'user' || i || '@example.com'
FROM generate_series(1, 100000) AS i
ON CONFLICT (customer_id) DO NOTHING;

-- Auths
INSERT INTO customer_auths (customer_id, pin_hash, biometric_enabled)
SELECT 
    'CUS-' || LPAD(i::text, 8, '0'),
    'hash123',
    FALSE
FROM generate_series(1, 100000) AS i
ON CONFLICT (customer_id) DO NOTHING;

-- Preferences
INSERT INTO customer_preferences (customer_id, language, notif_enabled, dark_mode)
SELECT 
    'CUS-' || LPAD(i::text, 8, '0'),
    'id',
    TRUE,
    FALSE
FROM generate_series(1, 100000) AS i
ON CONFLICT (customer_id) DO NOTHING;

-- Accounts
INSERT INTO accounts (account_no, customer_id, type, balance, currency)
SELECT 
    '123-456-' || LPAD(i::text, 6, '0'),
    'CUS-' || LPAD(i::text, 8, '0'),
    'savings',
    100000.0,
    'IDR'
FROM generate_series(1, 100000) AS i
ON CONFLICT (account_no) DO NOTHING;

-- Insert a few sample transactions for testing
INSERT INTO transactions (trx_id, account_no, type, amount, status, ref_no, created_at, updated_at)
VALUES 
('TRX-20260401-001', '123-456-000001', 'deposit', 50000.0, 'success', 'REF001', '2026-04-05 10:00:00', '2026-04-05 10:00:00'),
('TRX-20260401-002', '123-456-000002', 'withdraw', 20000.0, 'success', 'REF002', '2026-04-05 11:00:00', '2026-04-05 11:00:00'),
('TRX-20260401-003', '123-456-000003', 'transfer', 30000.0, 'success', 'REF003', '2026-04-06 12:00:00', '2026-04-06 12:00:00'),
('TRX-20260401-004', '123-456-000004', 'deposit', 100000.0, 'pending', 'REF004', '2026-04-06 13:00:00', '2026-04-06 13:00:00');