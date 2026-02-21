-- Seed dummy transactions kalau tabel kosong
INSERT INTO transactions (user_id, amount, status, created_at, updated_at)
SELECT 
    100 + generate_series(1, 5),  -- user_id 101 sampai 105
    (random() * 100000 + 10000)::double precision,  -- amount random 10k - 110k
    CASE 
        WHEN random() > 0.2 THEN 'success'
        WHEN random() > 0.6 THEN 'pending'
        ELSE 'failed'
    END,
    NOW() - interval '1 day' * random() * 30,  -- created_at random dalam 30 hari terakhir
    NOW()
ON CONFLICT DO NOTHING;