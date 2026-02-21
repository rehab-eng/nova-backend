from pathlib import Path
import re

path = Path("src/index.ts")
text = path.read_text(encoding="utf-8")

def replace_once(text, old, new, label):
    if old not in text:
        raise SystemExit(f"Missing block: {label}")
    return text.replace(old, new, 1)

def re_sub_once(text, pattern, repl, label):
    new_text, count = re.subn(pattern, repl, text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit(f"Missing block: {label}")
    return new_text

# StoreRow
old = '''type StoreRow = {
  id: string;
  name: string;
  admin_code: string | null;
  wallet_balance: number | null;
};'''
new = '''type StoreRow = {
  id: string;
  name: string;
  admin_code: string | null;
  store_code: string | null;
  wallet_balance: number | null;
};'''
text = replace_once(text, old, new, "StoreRow")

# OrderRow cash/wallet
old = "  delivery_fee: number | null;\n  status: string | null;\n"
new = "  delivery_fee: number | null;\n  cash_amount: number | null;\n  wallet_amount: number | null;\n  status: string | null;\n"
text = replace_once(text, old, new, "OrderRow cash/wallet")

# Insert helpers after randomCode
marker = "function randomCode(length = 6): string {"
idx = text.find(marker)
if idx < 0:
    raise SystemExit("Missing randomCode")
end = text.find("}\n\n", idx)
if end < 0:
    raise SystemExit("Missing end of randomCode")

helpers = '''
async function generateStoreCode(env: Env): Promise<string> {
  for (let i = 0; i < 8; i++) {
    const code = randomCode(8);
    const existing = await env.nova_max_db
      .prepare("SELECT id FROM stores WHERE store_code = ?")
      .bind(code)
      .first<{ id: string }>();
    if (!existing) return code;
  }
  throw new Error("Could not allocate store code");
}

async function generateDriverCode(env: Env): Promise<string> {
  for (let i = 0; i < 8; i++) {
    const code = randomCode(6);
    const existing = await env.nova_max_db
      .prepare("SELECT id FROM drivers WHERE secret_code = ?")
      .bind(code)
      .first<{ id: string }>();
    if (!existing) return code;
  }
  throw new Error("Could not allocate driver code");
}

async function ensureStoreCode(env: Env, store: StoreRow): Promise<StoreRow> {
  if (store.store_code) return store;
  const storeCode = await generateStoreCode(env);
  await env.nova_max_db
    .prepare("UPDATE stores SET store_code = ? WHERE id = ?")
    .bind(storeCode, store.id)
    .run();
  return { ...store, store_code: storeCode };
}

async function requireDriverByCode(
  env: Env,
  driverCode: string | null
): Promise<DriverRow | null> {
  if (!driverCode) return null;
  return await env.nova_max_db
    .prepare(
      "SELECT * FROM drivers WHERE secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
    )
    .bind(driverCode)
    .first<DriverRow>();
}

async function resolveDriverKey(env: Env, driverKey: string): Promise<DriverRow | null> {
  return await env.nova_max_db
    .prepare("SELECT * FROM drivers WHERE id = ? OR secret_code = ?")
    .bind(driverKey, driverKey)
    .first<DriverRow>();
}

'''
text = text[:end+3] + helpers + text[end+3:]

# requireStore (ensure store_code)
text = re_sub_once(
    text,
    r'async function requireStore[\s\S]*?\n}\n\nasync function requireDriver',
    '''async function requireStore(
  env: Env,
  storeId: string | null,
  adminCode: string | null
): Promise<StoreRow | null> {
  if (!adminCode) return null;
  let store: StoreRow | null = null;
  if (storeId) {
    store = await env.nova_max_db
      .prepare("SELECT * FROM stores WHERE id = ? AND admin_code = ?")
      .bind(storeId, adminCode)
      .first<StoreRow>();
  } else {
    store = await env.nova_max_db
      .prepare("SELECT * FROM stores WHERE admin_code = ?")
      .bind(adminCode)
      .first<StoreRow>();
  }
  if (!store) return null;
  return await ensureStoreCode(env, store);
}

async function requireDriver''',
    "requireStore"
)

# /realtime driver branch
old = '''      } else {
        driverId = getString(url.searchParams.get("driver_id"));
        const secretCode =
          getString(url.searchParams.get("secret_code")) ??
          getString(request.headers.get("x-driver-code"));
        if (!driverId || !secretCode) {
          return errorResponse("Missing driver_id or secret_code", 400, origin);
        }

        const driver = await requireDriver(env, driverId, secretCode);
        if (!driver) return errorResponse("Unauthorized", 401, origin);
        storeId = driver.store_id ?? null;
      }'''
new = '''      } else {
        const driverCode =
          getString(url.searchParams.get("driver_code")) ??
          getString(url.searchParams.get("secret_code")) ??
          getString(request.headers.get("x-driver-code"));
        const driverIdParam = getString(url.searchParams.get("driver_id"));

        if (!driverCode && !driverIdParam) {
          return errorResponse("Missing driver_code or driver_id", 400, origin);
        }

        let driver: DriverRow | null = null;

        if (driverCode) {
          driver = await requireDriverByCode(env, driverCode);
          if (!driver) return errorResponse("Unauthorized", 401, origin);
          if (driverIdParam && driver.id !== driverIdParam) {
            return errorResponse("Unauthorized", 401, origin);
          }
          driverId = driver.id;
        } else if (driverIdParam) {
          const secretCode =
            getString(url.searchParams.get("secret_code")) ??
            getString(request.headers.get("x-driver-code"));
          if (!secretCode) {
            return errorResponse("Missing driver_code", 400, origin);
          }
          driver = await requireDriver(env, driverIdParam, secretCode);
          if (!driver) return errorResponse("Unauthorized", 401, origin);
          driverId = driverIdParam;
        }

        storeId = driver?.store_id ?? null;
      }'''
text = replace_once(text, old, new, "/realtime driver branch")

# /stores create
old = '''    if (request.method === "POST" && path === "/stores") {
      const body = await request.json().catch(() => null);
      const name = getString(body?.name);
      if (!name) return errorResponse("Missing store name", 400, origin);

      const id = crypto.randomUUID();
      const adminCode = randomCode(8);

      await env.nova_max_db
        .prepare("INSERT INTO stores (id, name, admin_code) VALUES (?, ?, ?)")
        .bind(id, name, adminCode)
        .run();

      return jsonResponse(
        { ok: true, store: { id, name, admin_code: adminCode } },
        201,
        origin
      );
    }'''
new = '''    if (request.method === "POST" && path === "/stores") {
      const body = await request.json().catch(() => null);
      const name = getString(body?.name);
      if (!name) return errorResponse("Missing store name", 400, origin);

      const id = crypto.randomUUID();
      const adminCode = randomCode(8);
      const storeCode = await generateStoreCode(env);

      await env.nova_max_db
        .prepare("INSERT INTO stores (id, name, admin_code, store_code) VALUES (?, ?, ?, ?)")
        .bind(id, name, adminCode, storeCode)
        .run();

      return jsonResponse(
        { ok: true, store: { id, name, admin_code: adminCode, store_code: storeCode } },
        201,
        origin
      );
    }'''
text = replace_once(text, old, new, "/stores")

# /stores/by-admin
old = '''    if (request.method === "POST" && path === "/stores/by-admin") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);

      const store = await env.nova_max_db
        .prepare("SELECT id, name FROM stores WHERE admin_code = ?")
        .bind(adminCode)
        .first<{ id: string; name: string }>();

      if (!store) return errorResponse("Store not found", 404, origin);
      return jsonResponse({ ok: true, store }, 200, origin);
    }'''
new = '''    if (request.method === "POST" && path === "/stores/by-admin") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);

      const store = await env.nova_max_db
        .prepare("SELECT * FROM stores WHERE admin_code = ?")
        .bind(adminCode)
        .first<StoreRow>();

      if (!store) return errorResponse("Store not found", 404, origin);
      const ensured = await ensureStoreCode(env, store);

      return jsonResponse(
        {
          ok: true,
          store: {
            id: ensured.id,
            name: ensured.name,
            admin_code: ensured.admin_code,
            store_code: ensured.store_code,
          },
        },
        200,
        origin
      );
    }'''
text = replace_once(text, old, new, "/stores/by-admin")

# /drivers create
old = '''    if (request.method === "POST" && path === "/drivers") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const storeId = getString(body?.store_id);
      const name = getString(body?.name);
      const phone = getString(body?.phone);
      const photoUrl = getString(body?.photo_url);

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!name || !phone) {
        return errorResponse("Missing driver name or phone", 400, origin);
      }

      const store = await requireStore(env, storeId, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const existing = await env.nova_max_db
        .prepare("SELECT id FROM drivers WHERE phone = ?")
        .bind(phone)
        .first<{ id: string }>();
      if (existing) return errorResponse("Driver phone already exists", 409, origin);

      let lastError: unknown = null;
      for (let i = 0; i < 5; i++) {
        const secretCode = randomCode(6);
        try {
          const id = crypto.randomUUID();
          await env.nova_max_db
            .prepare(
              "INSERT INTO drivers (id, name, phone, secret_code, store_id, photo_url, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)"
            )
            .bind(id, name, phone, secretCode, store.id, photoUrl)
            .run();

          await broadcastEvent(env, store.id, {
            type: "driver_created",
            driver: {
              id,
              name,
              phone,
              store_id: store.id,
              photo_url: photoUrl,
              status: "offline",
              is_active: 1,
            },
            audience: "admin",
          });

          return jsonResponse(
            {
              ok: true,
              driver: {
                id,
                name,
                phone,
                store_id: store.id,
                secret_code: secretCode,
                photo_url: photoUrl,
              },
            },
            201,
            origin
          );
        } catch (err) {
          lastError = err;
        }
      }

      return errorResponse(
        "Could not allocate a unique driver code",
        500,
        origin
      );
    }'''
new = '''    if (request.method === "POST" && path === "/drivers") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const storeId = getString(body?.store_id);
      const name = getString(body?.name);
      const phone = getString(body?.phone);
      const photoUrl = getString(body?.photo_url);

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!name || !phone) {
        return errorResponse("Missing driver name or phone", 400, origin);
      }

      const store = await requireStore(env, storeId, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const existing = await env.nova_max_db
        .prepare("SELECT id FROM drivers WHERE phone = ?")
        .bind(phone)
        .first<{ id: string }>();
      if (existing) return errorResponse("Driver phone already exists", 409, origin);

      try {
        const secretCode = await generateDriverCode(env);
        const id = crypto.randomUUID();

        await env.nova_max_db
          .prepare(
            "INSERT INTO drivers (id, name, phone, secret_code, store_id, photo_url, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)"
          )
          .bind(id, name, phone, secretCode, store.id, photoUrl)
          .run();

        await broadcastEvent(env, store.id, {
          type: "driver_created",
          driver: {
            id,
            name,
            phone,
            store_id: store.id,
            photo_url: photoUrl,
            status: "offline",
            is_active: 1,
            driver_code: secretCode,
          },
          audience: "admin",
        });

        return jsonResponse(
          {
            ok: true,
            driver: {
              id,
              name,
              phone,
              store_id: store.id,
              driver_code: secretCode,
              secret_code: secretCode,
              photo_url: photoUrl,
            },
          },
          201,
          origin
        );
      } catch {
        return errorResponse("Could not allocate a unique driver code", 500, origin);
      }
    }'''
text = replace_once(text, old, new, "/drivers create")

# /drivers list add driver_code
text = text.replace(
  "SELECT id, name, phone, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE store_id = ?",
  "SELECT id, name, phone, status, wallet_balance, photo_url, store_id, is_active, secret_code FROM drivers WHERE store_id = ?",
  1
)
text = text.replace(
  "return jsonResponse({ ok: true, drivers: result.results ?? [] }, 200, origin);",
  "const drivers = (result.results ?? []).map((driver) => ({ ...driver, driver_code: driver.secret_code ?? null }));\n\n      return jsonResponse({ ok: true, drivers }, 200, origin);",
  1
)

# /drivers login include secret_code
text = text.replace(
  "SELECT id, name, phone, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE phone = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)",
  "SELECT id, name, phone, status, wallet_balance, photo_url, store_id, is_active, secret_code FROM drivers WHERE phone = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)",
  1
)

# /drivers active use driver_code
text = text.replace(
  "const driverId = segments[1];",
  "const driverKey = segments[1];",
  1
)
text = text.replace(
  'const existing = await env.nova_max_db\n        .prepare("SELECT id, store_id FROM drivers WHERE id = ?")\n        .bind(driverId)\n        .first<{ id: string; store_id: string | null }>();',
  'const existing = await resolveDriverKey(env, driverKey);',
  1
)
text = text.replace(
  "if (!existing) return errorResponse(\"Driver not found\", 404, origin);",
  "if (!existing) return errorResponse(\"Driver not found\", 404, origin);\n      const driverId = existing.id;",
  1
)

# /drivers delete purge
text = text.replace(
  "const driverId = segments[1];",
  "const driverKey = segments[1];",
  1
)
text = text.replace(
  'const existing = await env.nova_max_db\n        .prepare("SELECT id, store_id FROM drivers WHERE id = ?")\n        .bind(driverId)\n        .first<{ id: string; store_id: string | null }>();',
  "const existing = await resolveDriverKey(env, driverKey);",
  1
)
text = text.replace(
  "if (!existing) return errorResponse(\"Driver not found\", 404, origin);",
  "if (!existing) return errorResponse(\"Driver not found\", 404, origin);\n      const driverId = existing.id;",
  1
)

text = text.replace(
  "await env.nova_max_db\n        .prepare(\"UPDATE drivers SET is_active = 0, status = 'offline' WHERE id = ?\")\n        .bind(driverId)\n        .run();",
  "const purge = getString(url.searchParams.get(\"purge\")) ?? getString(body?.purge);\n      const shouldPurge = purge === \"1\" || purge === \"true\";\n\n      if (shouldPurge) {\n        await env.nova_max_db.batch([\n          env.nova_max_db.prepare(\"DELETE FROM wallet_transactions WHERE driver_id = ?\").bind(driverId),\n          env.nova_max_db.prepare(\"DELETE FROM orders WHERE driver_id = ?\").bind(driverId),\n          env.nova_max_db.prepare(\"DELETE FROM drivers WHERE id = ?\").bind(driverId),\n        ]);\n\n        return jsonResponse({ ok: true, purged: true }, 200, origin);\n      }\n\n      await env.nova_max_db\n        .prepare(\"UPDATE drivers SET is_active = 0, status = 'offline' WHERE id = ?\")\n        .bind(driverId)\n        .run();",
  1
)

# /stores delete (new)
insert_marker = "if (request.method === \"POST\" && path === \"/stores\") {"
insert_idx = text.find(insert_marker)
if insert_idx < 0:
    raise SystemExit("Missing /stores marker")
stores_by_admin_end = text.find("}\n\n    if (request.method === \"POST\" && path === \"/drivers\") {", insert_idx)
if stores_by_admin_end < 0:
    raise SystemExit("Missing /drivers marker")
stores_delete_block = '''
    if (request.method === "DELETE" && segments[0] === "stores" && segments.length === 2) {
      const storeKey = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ??
        getString(request.headers.get("x-admin-code")) ??
        getString(url.searchParams.get("admin_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);

      const store = await env.nova_max_db
        .prepare(
          "SELECT * FROM stores WHERE (id = ? OR store_code = ?) AND admin_code = ?"
        )
        .bind(storeKey, storeKey, adminCode)
        .first<StoreRow>();

      if (!store) return errorResponse("Store not found", 404, origin);

      const purge = getString(url.searchParams.get("purge")) ?? getString(body?.purge);
      const shouldPurge = purge === "1" || purge === "true";
      if (!shouldPurge) {
        return errorResponse("purge=1 required", 400, origin);
      }

      await env.nova_max_db.batch([
        env.nova_max_db
          .prepare(
            "DELETE FROM wallet_transactions WHERE driver_id IN (SELECT id FROM drivers WHERE store_id = ?)"
          )
          .bind(store.id),
        env.nova_max_db.prepare("DELETE FROM orders WHERE store_id = ?").bind(store.id),
        env.nova_max_db.prepare("DELETE FROM drivers WHERE store_id = ?").bind(store.id),
        env.nova_max_db.prepare("DELETE FROM stores WHERE id = ?").bind(store.id),
      ]);

      return jsonResponse({ ok: true, purged: true }, 200, origin);
    }

'''
text = text[:stores_by_admin_end] + stores_delete_block + text[stores_by_admin_end:]

# /orders create (driver_code + cash/wallet + store info)
text = text.replace(
  "const driverId = getString(body?.driver_id);",
  "const driverId = getString(body?.driver_id);\n      const driverCode = getString(body?.driver_code) ?? getString(request.headers.get(\"x-driver-code\"));",
  1
)

text = text.replace(
  "const store = await requireStore(env, storeId, adminCode);\n      if (!store) return errorResponse(\"Unauthorized\", 401, origin);\n      const effectiveStoreId = store.id;",
  "const store = await requireStore(env, storeId, adminCode);\n      if (!store) return errorResponse(\"Unauthorized\", 401, origin);\n      const effectiveStoreId = store.id;\n      const storeName = store.name;\n      const storeCode = store.store_code ?? null;\n      let assignedDriverId = driverId;\n      if (driverCode && !assignedDriverId) {\n        const driverByCode = await requireDriverByCode(env, driverCode);\n        if (!driverByCode) return errorResponse(\"Driver not found\", 404, origin);\n        if (driverByCode.store_id && driverByCode.store_id !== effectiveStoreId) {\n          return errorResponse(\"Driver not in store\", 409, origin);\n        }\n        assignedDriverId = driverByCode.id;\n      }\n      const safePrice = typeof price === \"number\" ? price : 0;\n      const cashAmount = payoutMethod === \"cash\" ? safePrice : 0;\n      const walletAmount = payoutMethod === \"cash\" ? 0 : safePrice;",
  1
)

text = text.replace(
  "INSERT INTO orders (id, store_id, driver_id, customer_name, customer_location_text, order_type, receiver_name, payout_method, product_image_url, price, delivery_fee, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  "INSERT INTO orders (id, store_id, driver_id, customer_name, customer_location_text, order_type, receiver_name, payout_method, product_image_url, price, delivery_fee, cash_amount, wallet_amount, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  1
)

text = text.replace(
  "orderId,\n          effectiveStoreId,\n          driverId,\n",
  "orderId,\n          effectiveStoreId,\n          assignedDriverId,\n",
  1
)

text = text.replace(
  "price,\n          deliveryFee,\n          \"pending\"",
  "price,\n          deliveryFee,\n          cashAmount,\n          walletAmount,\n          \"pending\"",
  1
)

text = text.replace(
  "target_driver_id: driverId ?? undefined,",
  "target_driver_id: assignedDriverId ?? undefined,",
  1
)

text = text.replace(
  "store_id: effectiveStoreId,\n          driver_id: driverId,\n",
  "store_id: effectiveStoreId,\n          store_name: storeName,\n          store_code: storeCode,\n          driver_id: assignedDriverId,\n",
  1
)

text = text.replace(
  "delivery_fee: deliveryFee,\n          status: \"pending\",\n          created_at: new Date().toISOString(),",
  "delivery_fee: deliveryFee,\n          cash_amount: cashAmount,\n          wallet_amount: walletAmount,\n          status: \"pending\",\n          created_at: new Date().toISOString(),",
  1
)

text = text.replace(
  "driver_id: driverId,\n            customer_name: customerName,",
  "driver_id: assignedDriverId,\n            customer_name: customerName,",
  1
)

text = text.replace(
  "delivery_fee: deliveryFee,\n            status: \"pending\",\n          },",
  "delivery_fee: deliveryFee,\n            cash_amount: cashAmount,\n            wallet_amount: walletAmount,\n            status: \"pending\",\n          },",
  1
)

# /orders status accept driver_code
text = text.replace(
  "const driverId =\n        getString(body?.driver_id) ?? getString(request.headers.get(\"x-driver-id\"));",
  "let driverId =\n        getString(body?.driver_id) ?? getString(request.headers.get(\"x-driver-id\"));\n      const driverCode =\n        getString(body?.driver_code) ??\n        getString(body?.secret_code) ??\n        getString(request.headers.get(\"x-driver-code\"));",
  1
)

text = text.replace(
  "const driver = await requireDriver(env, driverId, secretCode);\n\n      if (!store && !driver) return errorResponse(\"Unauthorized\", 401, origin);",
  "let driver = await requireDriver(env, driverId, secretCode);\n      if (!driver && driverCode) {\n        driver = await requireDriverByCode(env, driverCode);\n        if (driver && !driverId) driverId = driver.id;\n      }\n\n      if (!store && !driver) return errorResponse(\"Unauthorized\", 401, origin);",
  1
)

# ledger summary add cash/wallet sums
text = text.replace(
  "SUM(COALESCE(delivery_fee, 0)) AS delivery_total",
  "SUM(COALESCE(delivery_fee, 0)) AS delivery_total,\n                  SUM(COALESCE(cash_amount, 0)) AS cash_total,\n                  SUM(COALESCE(wallet_amount, 0)) AS wallet_total",
  1
)

# ledger drivers add cash/wallet sums
text = text.replace(
  "SUM(COALESCE(o.delivery_fee, 0)) AS delivery_total",
  "SUM(COALESCE(o.delivery_fee, 0)) AS delivery_total,\n                SUM(COALESCE(o.cash_amount, 0)) AS cash_total,\n                SUM(COALESCE(o.wallet_amount, 0)) AS wallet_total",
  1
)

path.write_text(text, encoding="utf-8")
print("OK")
