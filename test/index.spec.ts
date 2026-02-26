import { env, createExecutionContext, waitOnExecutionContext, SELF } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import worker from '../src/index';

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

describe('Nova backend', () => {
	const setupSchema = async () => {
		await env.nova_max_db.batch([
			env.nova_max_db.prepare("DROP TABLE IF EXISTS orders"),
			env.nova_max_db.prepare("DROP TABLE IF EXISTS store_wallet_transactions"),
			env.nova_max_db.prepare("DROP TABLE IF EXISTS wallet_transactions"),
			env.nova_max_db.prepare("DROP TABLE IF EXISTS drivers"),
			env.nova_max_db.prepare("DROP TABLE IF EXISTS stores"),
			env.nova_max_db.prepare(`CREATE TABLE IF NOT EXISTS stores (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        admin_code TEXT,
        store_code TEXT,
        wallet_balance REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`),
			env.nova_max_db.prepare(`CREATE TABLE IF NOT EXISTS drivers (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        phone TEXT,
        secret_code TEXT,
        status TEXT,
        wallet_balance REAL DEFAULT 0,
        store_id TEXT,
        is_active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`),
			env.nova_max_db.prepare(`CREATE TABLE IF NOT EXISTS wallet_transactions (
        id TEXT PRIMARY KEY,
        driver_id TEXT NOT NULL,
        amount REAL NOT NULL,
        type TEXT NOT NULL,
        method TEXT,
        note TEXT,
        idempotency_key TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`),
			env.nova_max_db.prepare(
				"CREATE UNIQUE INDEX IF NOT EXISTS wallet_tx_idem_unique ON wallet_transactions(idempotency_key)"
			),
			env.nova_max_db.prepare(`CREATE TABLE IF NOT EXISTS store_wallet_transactions (
        id TEXT PRIMARY KEY,
        store_id TEXT NOT NULL,
        amount REAL NOT NULL,
        type TEXT NOT NULL,
        method TEXT,
        note TEXT,
        idempotency_key TEXT,
        related_driver_id TEXT,
        related_order_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`),
			env.nova_max_db.prepare(
				"CREATE UNIQUE INDEX IF NOT EXISTS store_wallet_idem_unique ON store_wallet_transactions(idempotency_key)"
			),
		]);
	};

	it('returns not found on root (unit style)', async () => {
		const request = new IncomingRequest('http://example.com');
		const ctx = createExecutionContext();
		const response = await worker.fetch(request, env, ctx);
		await waitOnExecutionContext(ctx);
		expect(response.status).toBe(404);
		expect(await response.json()).toEqual({ ok: false, error: 'Not Found' });
	});

	it('returns not found on root (integration style)', async () => {
		const response = await SELF.fetch('https://example.com');
		expect(response.status).toBe(404);
		expect(await response.json()).toEqual({ ok: false, error: 'Not Found' });
	});

	it('rejects duplicate wallet idempotency keys (unit style)', async () => {
		await setupSchema();
		await env.nova_max_db.batch([
			env.nova_max_db
				.prepare(
					"INSERT OR REPLACE INTO stores (id, name, admin_code, store_code, wallet_balance) VALUES (?, ?, ?, ?, ?)"
				)
				.bind("store-1", "Nova Store", "1111", "1234", 0),
			env.nova_max_db
				.prepare(
					"INSERT OR REPLACE INTO drivers (id, name, phone, secret_code, store_id, wallet_balance) VALUES (?, ?, ?, ?, ?, ?)"
				)
				.bind("driver-1", "Driver One", "0900", "9999", "store-1", 0),
		]);

		const payload = {
			admin_code: "1111",
			amount: 25,
			method: "wallet",
			idempotency_key: "test-dup-001",
		};

		const request1 = new IncomingRequest("http://example.com/drivers/driver-1/wallet/credit", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(payload),
		});
		const ctx1 = createExecutionContext();
		const response1 = await worker.fetch(request1, env, ctx1);
		await waitOnExecutionContext(ctx1);
		expect(response1.status).toBe(200);

		const request2 = new IncomingRequest("http://example.com/drivers/driver-1/wallet/credit", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(payload),
		});
		const ctx2 = createExecutionContext();
		const response2 = await worker.fetch(request2, env, ctx2);
		await waitOnExecutionContext(ctx2);
		expect(response2.status).toBe(409);
		const body2 = await response2.json();
		expect(body2?.error?.toLowerCase()).toContain("constraint");
	});
});
