export interface Env {
  nova_max_db: D1Database;
  ALLOWED_ORIGIN?: string;
}

type OrderStatus = "pending" | "accepted" | "delivering" | "delivered" | "cancelled";

type StoreRow = {
  id: string;
  name: string;
  admin_code: string | null;
  wallet_balance: number | null;
};

type DriverRow = {
  id: string;
  name: string;
  phone: string | null;
  secret_code: string | null;
  status: string | null;
  wallet_balance: number | null;
  photo_url: string | null;
};

type OrderRow = {
  id: string;
  store_id: string | null;
  driver_id: string | null;
  customer_name: string | null;
  customer_location_text: string | null;
  order_type: string | null;
  receiver_name: string | null;
  payout_method: string | null;
  product_image_url: string | null;
  price: number | null;
  delivery_fee: number | null;
  status: string | null;
  created_at: string | null;
};

type WalletTxRow = {
  id: string;
  driver_id: string;
  amount: number;
  type: "credit" | "debit" | string;
  method: string | null;
  note: string | null;
  created_at: string | null;
};

const JSON_HEADERS = {
  "Content-Type": "application/json; charset=utf-8",
};

const ORDER_TRANSITIONS: Record<OrderStatus, OrderStatus[]> = {
  pending: ["accepted", "cancelled"],
  accepted: ["delivering", "cancelled"],
  delivering: ["delivered", "cancelled"],
  delivered: [],
  cancelled: [],
};

function isOrderStatus(value: string): value is OrderStatus {
  return (
    value === "pending" ||
    value === "accepted" ||
    value === "delivering" ||
    value === "delivered" ||
    value === "cancelled"
  );
}

function canTransition(from: OrderStatus, to: OrderStatus): boolean {
  return ORDER_TRANSITIONS[from].includes(to);
}

function getOrigin(request: Request, env: Env): string {
  return env.ALLOWED_ORIGIN ?? request.headers.get("Origin") ?? "*";
}

function corsHeaders(origin: string): HeadersInit {
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
    "Access-Control-Allow-Headers":
      "Content-Type, X-Admin-Code, X-Driver-Id, X-Driver-Code",
  };
}

function jsonResponse(data: unknown, status: number, origin: string): Response {
  const headers = new Headers(JSON_HEADERS);
  const cors = corsHeaders(origin);
  for (const [k, v] of Object.entries(cors)) headers.set(k, v);
  return new Response(JSON.stringify(data), { status, headers });
}

function errorResponse(message: string, status: number, origin: string): Response {
  return jsonResponse({ ok: false, error: message }, status, origin);
}

function getString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : null;
}

function parseNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function randomCode(length = 6): string {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  let code = "";
  for (const b of bytes) code += String(b % 10);
  return code;
}

async function requireStore(
  env: Env,
  storeId: string | null,
  adminCode: string | null
): Promise<StoreRow | null> {
  if (!adminCode) return null;
  if (storeId) {
    return await env.nova_max_db
      .prepare("SELECT * FROM stores WHERE id = ? AND admin_code = ?")
      .bind(storeId, adminCode)
      .first<StoreRow>();
  }

  return await env.nova_max_db
    .prepare("SELECT * FROM stores WHERE admin_code = ?")
    .bind(adminCode)
    .first<StoreRow>();
}

async function requireDriver(
  env: Env,
  driverId: string | null,
  secretCode: string | null
): Promise<DriverRow | null> {
  if (!driverId || !secretCode) return null;
  return await env.nova_max_db
    .prepare("SELECT * FROM drivers WHERE id = ? AND secret_code = ?")
    .bind(driverId, secretCode)
    .first<DriverRow>();
}

async function listOrders(
  env: Env,
  filters: {
    storeId?: string | null;
    driverId?: string | null;
    status?: string | null;
    limit?: number | null;
  }
): Promise<OrderRow[]> {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (filters.storeId) {
    clauses.push("store_id = ?");
    params.push(filters.storeId);
  }
  if (filters.driverId) {
    clauses.push("driver_id = ?");
    params.push(filters.driverId);
  }
  if (filters.status) {
    clauses.push("status = ?");
    params.push(filters.status);
  }

  let sql = "SELECT * FROM orders";
  if (clauses.length) sql += " WHERE " + clauses.join(" AND ");
  sql += " ORDER BY created_at DESC LIMIT ?";

  const limit = Math.min(Math.max(filters.limit ?? 200, 1), 500);
  params.push(limit);

  const result = await env.nova_max_db.prepare(sql).bind(...params).run<OrderRow>();
  return result.results as OrderRow[];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const origin = getOrigin(request, env);
    const path = url.pathname;
    const segments = path.split("/").filter(Boolean);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (request.method === "GET" && path === "/health") {
      return jsonResponse({ ok: true }, 200, origin);
    }

    if (request.method === "POST" && path === "/stores") {
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
    }

    if (request.method === "POST" && path === "/stores/by-admin") {
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
    }

    if (request.method === "POST" && path === "/drivers") {
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
              "INSERT INTO drivers (id, name, phone, secret_code, photo_url) VALUES (?, ?, ?, ?, ?)"
            )
            .bind(id, name, phone, secretCode, photoUrl)
            .run();

          return jsonResponse(
            {
              ok: true,
              driver: { id, name, phone, secret_code: secretCode, photo_url: photoUrl },
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
    }

    if (request.method === "POST" && path === "/drivers/login") {
      const body = await request.json().catch(() => null);
      const phone = getString(body?.phone);
      const secretCode = getString(body?.secret_code);

      if (!phone || !secretCode) {
        return errorResponse("Missing phone or secret_code", 400, origin);
      }

      const driver = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, status, wallet_balance, photo_url FROM drivers WHERE phone = ? AND secret_code = ?"
        )
        .bind(phone, secretCode)
        .first<DriverRow>();

      if (!driver) return errorResponse("Invalid credentials", 401, origin);

      return jsonResponse({ ok: true, driver }, 200, origin);
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "status"
    ) {
      const driverId = segments[1];
      const body = await request.json().catch(() => null);
      const secretCode =
        getString(body?.secret_code) ??
        getString(request.headers.get("x-driver-code"));
      const status = getString(body?.status);

      if (!secretCode || !status) {
        return errorResponse("Missing secret_code or status", 400, origin);
      }
      if (status !== "online" && status !== "offline") {
        return errorResponse("Invalid status", 400, origin);
      }

      const driver = await requireDriver(env, driverId, secretCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      await env.nova_max_db
        .prepare("UPDATE drivers SET status = ? WHERE id = ?")
        .bind(status, driverId)
        .run();

      return jsonResponse({ ok: true, driver_id: driverId, status }, 200, origin);
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "profile"
    ) {
      const driverId = segments[1];
      const body = await request.json().catch(() => null);
      const secretCode =
        getString(body?.secret_code) ??
        getString(request.headers.get("x-driver-code"));
      const photoUrl = getString(body?.photo_url);
      const name = getString(body?.name);

      if (!secretCode) return errorResponse("Missing secret_code", 400, origin);
      if (!photoUrl && !name) {
        return errorResponse("Missing profile data", 400, origin);
      }

      const driver = await requireDriver(env, driverId, secretCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      const fields: string[] = [];
      const params: unknown[] = [];
      if (photoUrl) {
        fields.push("photo_url = ?");
        params.push(photoUrl);
      }
      if (name) {
        fields.push("name = ?");
        params.push(name);
      }
      params.push(driverId);

      await env.nova_max_db
        .prepare(`UPDATE drivers SET ${fields.join(", ")} WHERE id = ?`)
        .bind(...params)
        .run();

      const updated = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, status, wallet_balance, photo_url FROM drivers WHERE id = ?"
        )
        .bind(driverId)
        .first<DriverRow>();

      return jsonResponse({ ok: true, driver: updated }, 200, origin);
    }

    if (
      request.method === "GET" &&
      segments[0] === "drivers" &&
      segments.length === 2
    ) {
      const driverId = segments[1];
      const secretCode =
        getString(url.searchParams.get("secret_code")) ??
        getString(request.headers.get("x-driver-code"));
      if (!secretCode) return errorResponse("Missing secret_code", 400, origin);

      const driver = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, status, wallet_balance, photo_url FROM drivers WHERE id = ? AND secret_code = ?"
        )
        .bind(driverId, secretCode)
        .first<DriverRow>();

      if (!driver) return errorResponse("Unauthorized", 401, origin);
      return jsonResponse({ ok: true, driver }, 200, origin);
    }

    if (
      request.method === "DELETE" &&
      segments[0] === "drivers" &&
      segments.length === 2
    ) {
      const driverId = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ??
        getString(request.headers.get("x-admin-code")) ??
        getString(url.searchParams.get("admin_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const existing = await env.nova_max_db
        .prepare("SELECT id FROM drivers WHERE id = ?")
        .bind(driverId)
        .first<{ id: string }>();
      if (!existing) return errorResponse("Driver not found", 404, origin);

      await env.nova_max_db
        .prepare("UPDATE orders SET driver_id = NULL WHERE driver_id = ?")
        .bind(driverId)
        .run();
      await env.nova_max_db
        .prepare("DELETE FROM wallet_transactions WHERE driver_id = ?")
        .bind(driverId)
        .run();
      await env.nova_max_db
        .prepare("DELETE FROM drivers WHERE id = ?")
        .bind(driverId)
        .run();

      return jsonResponse({ ok: true }, 200, origin);
    }

    if (
      request.method === "POST" &&
      segments[0] === "drivers" &&
      segments.length === 4 &&
      segments[2] === "wallet" &&
      (segments[3] === "credit" || segments[3] === "debit")
    ) {
      const driverId = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const amount = parseNumber(body?.amount);
      const method = getString(body?.method);
      const note = getString(body?.note);
      const type = segments[3] === "credit" ? "credit" : "debit";

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      if (!amount || amount <= 0) {
        return errorResponse("Invalid amount", 400, origin);
      }

      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const driver = await env.nova_max_db
        .prepare("SELECT id, wallet_balance FROM drivers WHERE id = ?")
        .bind(driverId)
        .first<DriverRow>();
      if (!driver) return errorResponse("Driver not found", 404, origin);

      const current = typeof driver.wallet_balance === "number" ? driver.wallet_balance : 0;
      const next = type === "credit" ? current + amount : current - amount;
      if (next < 0) {
        return errorResponse("Insufficient wallet balance", 400, origin);
      }

      await env.nova_max_db
        .prepare("UPDATE drivers SET wallet_balance = ? WHERE id = ?")
        .bind(next, driverId)
        .run();

      const txId = crypto.randomUUID();
      await env.nova_max_db
        .prepare(
          "INSERT INTO wallet_transactions (id, driver_id, amount, type, method, note) VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(txId, driverId, amount, type, method, note)
        .run();

      return jsonResponse({ ok: true, balance: next }, 200, origin);
    }

    if (
      request.method === "GET" &&
      segments[0] === "drivers" &&
      segments.length === 4 &&
      segments[2] === "wallet" &&
      segments[3] === "transactions"
    ) {
      const driverId = segments[1];
      const adminCode =
        getString(url.searchParams.get("admin_code")) ??
        getString(request.headers.get("x-admin-code"));
      const secretCode =
        getString(url.searchParams.get("secret_code")) ??
        getString(request.headers.get("x-driver-code"));
      const limit = parseNumber(url.searchParams.get("limit"));

      let authorized = false;
      if (adminCode) {
        const store = await requireStore(env, null, adminCode);
        authorized = !!store;
      }
      if (!authorized && secretCode) {
        const driver = await requireDriver(env, driverId, secretCode);
        authorized = !!driver;
      }
      if (!authorized) return errorResponse("Unauthorized", 401, origin);

      const safeLimit = Math.min(Math.max(limit ?? 20, 1), 100);
      const result = await env.nova_max_db
        .prepare(
          "SELECT id, driver_id, amount, type, method, note, created_at FROM wallet_transactions WHERE driver_id = ? ORDER BY created_at DESC LIMIT ?"
        )
        .bind(driverId, safeLimit)
        .run<WalletTxRow>();

      return jsonResponse({ ok: true, transactions: result.results ?? [] }, 200, origin);
    }

    if (request.method === "GET" && path === "/orders/stream") {
      let storeId = getString(url.searchParams.get("store_id"));
      const driverId = getString(url.searchParams.get("driver_id"));
      const status = getString(url.searchParams.get("status"));
      const adminCode = getString(url.searchParams.get("admin_code"));

      if (!storeId && adminCode) {
        const store = await requireStore(env, null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      }

      if (!storeId && !driverId) {
        return errorResponse("store_id or driver_id required", 400, origin);
      }

      const encoder = new TextEncoder();
      let aborted = false;

      const stream = new ReadableStream({
        async start(controller) {
          request.signal.addEventListener("abort", () => {
            aborted = true;
            controller.close();
          });

          while (!aborted) {
            const orders = await listOrders(env, {
              storeId,
              driverId,
              status,
              limit: 200,
            });

            const payload = `event: orders\ndata: ${JSON.stringify(orders)}\n\n`;
            controller.enqueue(encoder.encode(payload));
            await sleep(2000);
          }
        },
      });

      const headers = new Headers({
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      const cors = corsHeaders(origin);
      for (const [k, v] of Object.entries(cors)) headers.set(k, v);

      return new Response(stream, { headers });
    }

    if (request.method === "POST" && path === "/orders") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const storeId = getString(body?.store_id);
      const driverId = getString(body?.driver_id);
      const customerName = getString(body?.customer_name);
      const customerLocation = getString(body?.customer_location_text);
      const orderType = getString(body?.order_type);
      const receiverName = getString(body?.receiver_name);
      const payoutMethod = getString(body?.payout_method);
      const price = parseNumber(body?.price);
      const deliveryFee = parseNumber(body?.delivery_fee);
      const productImageUrl: string | null = null;

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!customerName || !customerLocation) {
        return errorResponse("Missing customer_name or customer_location_text", 400, origin);
      }

      const store = await requireStore(env, storeId, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);
      const effectiveStoreId = store.id;

      const orderId = crypto.randomUUID();

      await env.nova_max_db
        .prepare(
          "INSERT INTO orders (id, store_id, driver_id, customer_name, customer_location_text, order_type, receiver_name, payout_method, product_image_url, price, delivery_fee, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(
          orderId,
          effectiveStoreId,
          driverId,
          customerName,
          customerLocation,
          orderType,
          receiverName,
          payoutMethod,
          productImageUrl,
          price,
          deliveryFee,
          "pending"
        )
        .run();

      return jsonResponse(
        {
          ok: true,
          order: {
            id: orderId,
            store_id: effectiveStoreId,
            driver_id: driverId,
            customer_name: customerName,
            customer_location_text: customerLocation,
            order_type: orderType,
            receiver_name: receiverName,
            payout_method: payoutMethod,
            product_image_url: productImageUrl,
            price,
            delivery_fee: deliveryFee,
            status: "pending",
          },
        },
        201,
        origin
      );
    }

    if (request.method === "GET" && path === "/orders") {
      let storeId = getString(url.searchParams.get("store_id"));
      const adminCode = getString(url.searchParams.get("admin_code"));
      const driverId = getString(url.searchParams.get("driver_id"));
      const status = getString(url.searchParams.get("status"));
      const limit = parseNumber(url.searchParams.get("limit"));

      if (!storeId && adminCode) {
        const store = await requireStore(env, null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      }

      const orders = await listOrders(env, { storeId, driverId, status, limit });
      return jsonResponse({ ok: true, orders }, 200, origin);
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "orders" &&
      segments.length === 3 &&
      segments[2] === "status"
    ) {
      const orderId = segments[1];
      const body = await request.json().catch(() => null);
      const nextStatusRaw = getString(body?.status);
      if (!nextStatusRaw || !isOrderStatus(nextStatusRaw)) {
        return errorResponse("Invalid status", 400, origin);
      }
      const nextStatus = nextStatusRaw as OrderStatus;

      const storeId = getString(body?.store_id);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const driverId =
        getString(body?.driver_id) ?? getString(request.headers.get("x-driver-id"));
      const secretCode =
        getString(body?.secret_code) ?? getString(request.headers.get("x-driver-code"));

      const store = await requireStore(env, storeId, adminCode);
      const driver = await requireDriver(env, driverId, secretCode);

      if (!store && !driver) return errorResponse("Unauthorized", 401, origin);

      const order = await env.nova_max_db
        .prepare("SELECT * FROM orders WHERE id = ?")
        .bind(orderId)
        .first<OrderRow>();

      if (!order) return errorResponse("Order not found", 404, origin);
      if (!order.status || !isOrderStatus(order.status)) {
        return errorResponse("Order has invalid status", 400, origin);
      }

      const currentStatus = order.status as OrderStatus;
      if (currentStatus !== nextStatus && !canTransition(currentStatus, nextStatus)) {
        return errorResponse("Invalid status transition", 400, origin);
      }

      const effectiveDriverId = driverId ?? order.driver_id;
      if (
        (nextStatus === "accepted" ||
          nextStatus === "delivering" ||
          nextStatus === "delivered") &&
        !effectiveDriverId
      ) {
        return errorResponse("driver_id required for this status", 400, origin);
      }

      if (order.driver_id && effectiveDriverId && order.driver_id !== effectiveDriverId) {
        return errorResponse("Order already assigned to another driver", 409, origin);
      }

      await env.nova_max_db
        .prepare("UPDATE orders SET status = ?, driver_id = ? WHERE id = ?")
        .bind(nextStatus, effectiveDriverId, orderId)
        .run();

      if (
        nextStatus === "delivered" &&
        currentStatus !== "delivered" &&
        effectiveDriverId
      ) {
        const fee = typeof order.delivery_fee === "number" ? order.delivery_fee : 0;
        const payoutMethod = order.payout_method ?? "wallet";
        if (fee > 0 && payoutMethod === "wallet") {
          await env.nova_max_db
            .prepare("UPDATE drivers SET wallet_balance = wallet_balance + ? WHERE id = ?")
            .bind(fee, effectiveDriverId)
            .run();
        }
      }

      return jsonResponse(
        { ok: true, order_id: orderId, status: nextStatus },
        200,
        origin
      );
    }

    return errorResponse("Not Found", 404, origin);
  },
} satisfies ExportedHandler<Env>;
