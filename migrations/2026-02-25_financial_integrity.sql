ALTER TABLE orders ADD COLUMN customer_phone TEXT;
ALTER TABLE orders ADD COLUMN cancel_reason TEXT;
ALTER TABLE orders ADD COLUMN cancelled_by TEXT;
ALTER TABLE orders ADD COLUMN cancelled_at DATETIME;

ALTER TABLE wallet_transactions ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS wallet_tx_idem_unique ON wallet_transactions(idempotency_key);

CREATE TABLE IF NOT EXISTS store_wallet_transactions (
  id TEXT PRIMARY KEY,
  store_id TEXT NOT NULL,
  amount REAL NOT NULL,
  type TEXT NOT NULL,
  method TEXT,
  note TEXT,
  idempotency_key TEXT,
  related_driver_id TEXT,
  related_order_id TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(store_id) REFERENCES stores(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS store_wallet_idem_unique ON store_wallet_transactions(idempotency_key);
CREATE INDEX IF NOT EXISTS store_wallet_store_idx ON store_wallet_transactions(store_id);
