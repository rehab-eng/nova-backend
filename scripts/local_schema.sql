CREATE TABLE IF NOT EXISTS stores (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  admin_code TEXT,
  store_code TEXT,
  wallet_balance REAL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS stores_code_unique ON stores(store_code);

CREATE TABLE IF NOT EXISTS drivers (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  phone TEXT,
  secret_code TEXT,
  status TEXT,
  wallet_balance REAL DEFAULT 0,
  store_id TEXT,
  is_active INTEGER DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(store_id) REFERENCES stores(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS drivers_code_unique ON drivers(secret_code);
CREATE UNIQUE INDEX IF NOT EXISTS drivers_phone_unique ON drivers(phone);
CREATE INDEX IF NOT EXISTS drivers_store_idx ON drivers(store_id);

CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  store_id TEXT,
  driver_id TEXT,
  customer_name TEXT,
  customer_phone TEXT,
  customer_location_text TEXT,
  order_type TEXT,
  receiver_name TEXT,
  payout_method TEXT,
  price REAL,
  delivery_fee REAL,
  cash_amount REAL,
  wallet_amount REAL,
  status TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  delivered_at DATETIME,
  cancelled_at DATETIME,
  cancel_reason TEXT,
  cancelled_by TEXT,
  FOREIGN KEY(store_id) REFERENCES stores(id),
  FOREIGN KEY(driver_id) REFERENCES drivers(id)
);
CREATE INDEX IF NOT EXISTS orders_store_idx ON orders(store_id);
CREATE INDEX IF NOT EXISTS orders_driver_idx ON orders(driver_id);

CREATE TABLE IF NOT EXISTS wallet_transactions (
  id TEXT PRIMARY KEY,
  driver_id TEXT NOT NULL,
  amount REAL NOT NULL,
  type TEXT NOT NULL,
  method TEXT,
  note TEXT,
  idempotency_key TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(driver_id) REFERENCES drivers(id)
);
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
