-- Seed dummy users dengan balance awal
INSERT INTO users (username, email, balance, created_at, updated_at)
VALUES
    ('user1', 'user1@example.com', 100000.0, NOW() - interval '10 days', NOW()),
    ('user2', 'user2@example.com', 50000.0, NOW() - interval '5 days', NOW()),
    ('user3', 'user3@example.com', 200000.0, NOW() - interval '2 days', NOW()),
    ('user4', 'user4@example.com', 0.0, NOW() - interval '1 day', NOW()),
    ('user5', 'user5@example.com', 150000.0, NOW(), NOW())
ON CONFLICT (username) DO NOTHING;

-- Seed dummy transactions (mix deposit, withdraw, transfer)
INSERT INTO transactions (user_id, recipient_id, amount, type, status, description, created_at, updated_at)
SELECT 
    u.id AS user_id,
    CASE 
        WHEN random() > 0.7 THEN (SELECT id FROM users ORDER BY random() LIMIT 1) -- transfer ke user random
        ELSE NULL
    END AS recipient_id,
    (random() * 100000 + 10000)::double precision AS amount,
    CASE 
        WHEN random() > 0.6 THEN 'deposit'
        WHEN random() > 0.3 THEN 'withdraw'
        ELSE 'transfer'
    END AS type,
    CASE 
        WHEN random() > 0.2 THEN 'success'
        WHEN random() > 0.6 THEN 'pending'
        ELSE 'failed'
    END AS status,
    CASE 
        WHEN random() > 0.5 THEN 'Topup bulanan' 
        WHEN random() > 0.7 THEN 'Tarik tunai ATM' 
        ELSE 'Transfer ke teman' 
    END AS description,
    NOW() - interval '1 day' * (random() * 30)::int AS created_at,
    NOW() AS updated_at
FROM users u
CROSS JOIN generate_series(1, 3) -- 3 transaksi per user (total ~15 dummy)
ON CONFLICT DO NOTHING;