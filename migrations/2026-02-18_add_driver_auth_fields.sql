CREATE UNIQUE INDEX IF NOT EXISTS drivers_email_unique ON drivers(email);
CREATE INDEX IF NOT EXISTS drivers_store_idx ON drivers(store_id);

UPDATE drivers SET is_active = 1 WHERE is_active IS NULL;
UPDATE drivers
SET store_id = (SELECT id FROM stores LIMIT 1)
WHERE store_id IS NULL
  AND EXISTS (SELECT 1 FROM stores);

UPDATE orders
SET delivered_at = COALESCE(delivered_at, created_at)
WHERE status = 'delivered' AND delivered_at IS NULL;
