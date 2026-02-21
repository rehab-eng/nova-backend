ALTER TABLE stores ADD COLUMN store_code TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS stores_code_unique ON stores(store_code);

CREATE UNIQUE INDEX IF NOT EXISTS drivers_code_unique ON drivers(secret_code);

ALTER TABLE orders ADD COLUMN cash_amount REAL;
ALTER TABLE orders ADD COLUMN wallet_amount REAL;

UPDATE stores
SET store_code = admin_code
WHERE store_code IS NULL AND admin_code IS NOT NULL;
