
CREATE TABLE IF NOT EXISTS wallet_transactions (
  id TEXT PRIMARY KEY,
  driver_id TEXT NOT NULL,
  amount REAL NOT NULL,
  type TEXT NOT NULL,
  method TEXT,
  note TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(driver_id) REFERENCES drivers(id)
);

