export interface Env {
  nova_max_db: D1Database;
  REALTIME: DurableObjectNamespace;
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
  email: string | null;
  secret_code: string | null;
  status: string | null;
  wallet_balance: number | null;
  store_id: string | null;
  photo_url: string | null;
  is_active: number | null;
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
  delivered_at: string | null;
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

function getEmail(value: unknown): string | null {
  const email = getString(value)?.toLowerCase();
  if (!email) return null;
  if (!email.includes("@") || email.length < 5) return null;
  return email;
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
    .prepare(
      "SELECT * FROM drivers WHERE id = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
    )
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

async function broadcastEvent(
  env: Env,
  storeId: string,
  event: Record<string, unknown>
): Promise<void> {
  try {
    const id = env.REALTIME.idFromName(storeId);
    const stub = env.REALTIME.get(id);
    await stub.fetch("https://realtime/event", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
    });
  } catch (err) {
    console.warn("Realtime broadcast failed", err);
  }
}

function periodExpr(period: string | null): string {
  if (period === "weekly") return "%Y-%W";
  if (period === "monthly") return "%Y-%m";
  return "%Y-%m-%d";
}

type RealtimeRole = "admin" | "driver" | "guest";

type ConnectionMeta = {
  role: RealtimeRole;
  driverId: string | null;
};

export class RealtimeRoom {
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/event") {
      const payload = await request.json().catch(() => null);
      if (!payload || typeof payload !== "object") {
        return new Response("Invalid payload", { status: 400 });
      }
      this.broadcast(payload as Record<string, unknown>);
      return new Response(null, { status: 202 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected websocket", { status: 426 });
    }

    const roleHeader = request.headers.get("X-Role") ?? "guest";
    const role: RealtimeRole =
      roleHeader === "admin" || roleHeader === "driver" ? roleHeader : "guest";
    const driverId = request.headers.get("X-Driver-Id");

    const alreadyOnline = driverId ? this.hasDriverConnection(driverId) : false;

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);
    const meta: ConnectionMeta = { role, driverId: driverId ?? null };
    server.serializeAttachment(meta);

    if (driverId && !alreadyOnline) {
      await this.setDriverStatus(driverId, "online");
    }

    server.send(
      JSON.stringify({ type: "connected", role, ts: new Date().toISOString() })
    );

    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (typeof message !== "string") return;
    let payload: { type?: string } | null = null;
    try {
      payload = JSON.parse(message) as { type?: string };
    } catch {
      payload = null;
    }
    if (payload?.type === "ping") {
      try {
        ws.send(JSON.stringify({ type: "pong", ts: Date.now() }));
      } catch {
        // ignore
      }
    }
  }

  webSocketClose(ws: WebSocket): void {
    void this.handleDisconnect(ws);
  }

  webSocketError(ws: WebSocket): void {
    void this.handleDisconnect(ws);
  }

  private getMeta(ws: WebSocket): ConnectionMeta {
    const attachment = ws.deserializeAttachment() as ConnectionMeta | undefined;
    if (
      attachment &&
      (attachment.role === "admin" ||
        attachment.role === "driver" ||
        attachment.role === "guest")
    ) {
      return {
        role: attachment.role,
        driverId: attachment.driverId ?? null,
      };
    }
    return { role: "guest", driverId: null };
  }

  private hasDriverConnection(driverId: string, exclude?: WebSocket): boolean {
    for (const socket of this.state.getWebSockets()) {
      if (socket === exclude) continue;
      const meta = this.getMeta(socket);
      if (meta.driverId === driverId) return true;
    }
    return false;
  }

  private async handleDisconnect(ws: WebSocket): Promise<void> {
    const meta = this.getMeta(ws);
    if (!meta.driverId) return;
    const stillOnline = this.hasDriverConnection(meta.driverId, ws);
    if (!stillOnline) {
      await this.setDriverStatus(meta.driverId, "offline");
    }
  }

  private async setDriverStatus(
    driverId: string,
    status: "online" | "offline"
  ): Promise<void> {
    await this.env.nova_max_db
      .prepare("UPDATE drivers SET status = ? WHERE id = ?")
      .bind(status, driverId)
      .run();

    this.broadcast({
      type: "driver_status",
      driver_id: driverId,
      status,
      ts: new Date().toISOString(),
      audience: "admin",
    });
  }

  private broadcast(event: Record<string, unknown>): void {
    const message = JSON.stringify(event);
    const targetDriver = getString((event as { target_driver_id?: unknown }).target_driver_id);
    const audience = getString((event as { audience?: unknown }).audience);

    for (const socket of this.state.getWebSockets()) {
      const meta = this.getMeta(socket);
      if (audience === "admin" && meta.role !== "admin") continue;
      if (audience === "driver" && meta.role !== "driver") continue;
      if (targetDriver && meta.driverId !== targetDriver && meta.role !== "admin") {
        continue;
      }
      try {
        socket.send(message);
      } catch {
        // ignore send failures
      }
    }
  }
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

    if (request.method === "GET" && path === "/realtime") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return errorResponse("Expected websocket", 426, origin);
      }

      const role = getString(url.searchParams.get("role"));
      if (role !== "admin" && role !== "driver") {
        return errorResponse("Invalid role", 400, origin);
      }

      let storeId: string | null = null;
      let driverId: string | null = null;

      if (role === "admin") {
        const adminCode =
          getString(url.searchParams.get("admin_code")) ??
          getString(request.headers.get("x-admin-code"));
        if (!adminCode) return errorResponse("Missing admin_code", 400, origin);

        const store = await requireStore(env, null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      } else {
        driverId = getString(url.searchParams.get("driver_id"));
        const secretCode =
          getString(url.searchParams.get("secret_code")) ??
          getString(request.headers.get("x-driver-code"));
        const email =
          getEmail(url.searchParams.get("email")) ??
          getEmail(request.headers.get("x-driver-email"));

        if (!driverId || !secretCode || !email) {
          return errorResponse("Missing driver_id, secret_code, or email", 400, origin);
        }

        const driver = await requireDriver(env, driverId, secretCode);
        if (!driver) return errorResponse("Unauthorized", 401, origin);
        if (!driver.email || driver.email !== email) {
          return errorResponse("Email mismatch", 401, origin);
        }
        storeId = driver.store_id ?? null;
      }

      if (!storeId) return errorResponse("Missing store_id", 400, origin);

      const id = env.REALTIME.idFromName(storeId);
      const stub = env.REALTIME.get(id);
      const headers = new Headers(request.headers);
      headers.set("X-Role", role);
      headers.set("X-Store-Id", storeId);
      if (driverId) headers.set("X-Driver-Id", driverId);

      const realtimeRequest = new Request("https://realtime/connect", {
        method: "GET",
        headers,
      });

      return stub.fetch(realtimeRequest);
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
      const email = getEmail(body?.email);
      const photoUrl = getString(body?.photo_url);

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!name || !phone || !email) {
        return errorResponse("Missing driver name, phone, or email", 400, origin);
      }

      const store = await requireStore(env, storeId, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const existing = await env.nova_max_db
        .prepare("SELECT id FROM drivers WHERE phone = ?")
        .bind(phone)
        .first<{ id: string }>();
      if (existing) return errorResponse("Driver phone already exists", 409, origin);

      const existingEmail = await env.nova_max_db
        .prepare("SELECT id FROM drivers WHERE email = ?")
        .bind(email)
        .first<{ id: string }>();
      if (existingEmail) return errorResponse("Driver email already exists", 409, origin);

      let lastError: unknown = null;
      for (let i = 0; i < 5; i++) {
        const secretCode = randomCode(6);
        try {
          const id = crypto.randomUUID();
          await env.nova_max_db
            .prepare(
              "INSERT INTO drivers (id, name, phone, email, secret_code, store_id, photo_url, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, 1)"
            )
            .bind(id, name, phone, email, secretCode, store.id, photoUrl)
            .run();

          await broadcastEvent(env, store.id, {
            type: "driver_created",
            driver: {
              id,
              name,
              phone,
              email,
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
                email,
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
    }

    if (request.method === "GET" && path === "/drivers") {
      const adminCode =
        getString(url.searchParams.get("admin_code")) ??
        getString(request.headers.get("x-admin-code"));
      const activeParam = getString(url.searchParams.get("active"));
      const limit = parseNumber(url.searchParams.get("limit"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      let sql =
        "SELECT id, name, phone, email, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE store_id = ?";
      if (activeParam !== "all") {
        if (activeParam === "0" || activeParam === "false") {
          sql += " AND is_active = 0";
        } else {
          sql += " AND (is_active = 1 OR is_active IS NULL)";
        }
      }
      sql += " ORDER BY name ASC";

      const safeLimit = Math.min(Math.max(limit ?? 200, 1), 500);
      sql += " LIMIT ?";

      const result = await env.nova_max_db
        .prepare(sql)
        .bind(store.id, safeLimit)
        .run<DriverRow>();

      return jsonResponse({ ok: true, drivers: result.results ?? [] }, 200, origin);
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "active"
    ) {
      const driverId = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ??
        getString(request.headers.get("x-admin-code")) ??
        getString(url.searchParams.get("admin_code"));
      const activeNum = parseNumber(body?.active);
      const activeBool =
        typeof body?.active === "boolean"
          ? body.active
          : activeNum !== null
          ? activeNum !== 0
          : null;

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      if (activeBool === null) return errorResponse("Missing active flag", 400, origin);

      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const existing = await env.nova_max_db
        .prepare("SELECT id, store_id FROM drivers WHERE id = ?")
        .bind(driverId)
        .first<{ id: string; store_id: string | null }>();
      if (!existing) return errorResponse("Driver not found", 404, origin);
      if (existing.store_id && existing.store_id !== store.id) {
        return errorResponse("Unauthorized", 401, origin);
      }

      await env.nova_max_db
        .prepare(
          "UPDATE drivers SET is_active = ?, status = ? WHERE id = ?"
        )
        .bind(activeBool ? 1 : 0, activeBool ? "offline" : "offline", driverId)
        .run();

      await broadcastEvent(env, store.id, {
        type: activeBool ? "driver_active" : "driver_disabled",
        driver_id: driverId,
        ts: new Date().toISOString(),
        audience: "admin",
      });

      return jsonResponse(
        { ok: true, driver_id: driverId, active: activeBool },
        200,
        origin
      );
    }

    if (request.method === "POST" && path === "/drivers/login") {
      const body = await request.json().catch(() => null);
      const phone = getString(body?.phone);
      const email = getEmail(body?.email);
      const secretCode = getString(body?.secret_code);

      if (!phone || !email || !secretCode) {
        return errorResponse("Missing phone, email, or secret_code", 400, origin);
      }

      const driver = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, email, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE phone = ? AND email = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
        )
        .bind(phone, email, secretCode)
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

      if (driver.store_id) {
        await broadcastEvent(env, driver.store_id, {
          type: "driver_status",
          driver_id: driverId,
          status,
          ts: new Date().toISOString(),
          audience: "admin",
        });
      }

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
      const email = getEmail(body?.email);

      if (!secretCode) return errorResponse("Missing secret_code", 400, origin);
      if (!photoUrl && !name && !email) {
        return errorResponse("Missing profile data", 400, origin);
      }

      const driver = await requireDriver(env, driverId, secretCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      if (email && email !== driver.email) {
        const existingEmail = await env.nova_max_db
          .prepare("SELECT id FROM drivers WHERE email = ? AND id != ?")
          .bind(email, driverId)
          .first<{ id: string }>();
        if (existingEmail) {
          return errorResponse("Driver email already exists", 409, origin);
        }
      }

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
      if (email) {
        fields.push("email = ?");
        params.push(email);
      }
      params.push(driverId);

      await env.nova_max_db
        .prepare(`UPDATE drivers SET ${fields.join(", ")} WHERE id = ?`)
        .bind(...params)
        .run();

      const updated = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, email, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE id = ?"
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
          "SELECT id, name, phone, email, status, wallet_balance, photo_url, store_id, is_active FROM drivers WHERE id = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
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
        .prepare("SELECT id, store_id FROM drivers WHERE id = ?")
        .bind(driverId)
        .first<{ id: string; store_id: string | null }>();
      if (!existing) return errorResponse("Driver not found", 404, origin);
      if (existing.store_id && existing.store_id !== store.id) {
        return errorResponse("Unauthorized", 401, origin);
      }

      await env.nova_max_db
        .prepare("UPDATE drivers SET is_active = 0, status = 'offline' WHERE id = ?")
        .bind(driverId)
        .run();

      await broadcastEvent(env, store.id, {
        type: "driver_disabled",
        driver_id: driverId,
        ts: new Date().toISOString(),
        audience: "admin",
      });

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
        .prepare("SELECT id, wallet_balance, store_id FROM drivers WHERE id = ?")
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

      if (driver.store_id) {
        await broadcastEvent(env, driver.store_id, {
          type: "wallet_transaction",
          driver_id: driverId,
          balance: next,
          target_driver_id: driverId,
          transaction: {
            id: txId,
            amount,
            type,
            method,
            note,
            created_at: new Date().toISOString(),
          },
        });
      }

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

    if (request.method === "GET" && path === "/ledger/summary") {
      const adminCode =
        getString(url.searchParams.get("admin_code")) ??
        getString(request.headers.get("x-admin-code"));
      const period = getString(url.searchParams.get("period"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const format = periodExpr(period);

      const ordersResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', COALESCE(delivered_at, created_at)) AS period,
                  COUNT(*) AS trips,
                  SUM(COALESCE(delivery_fee, 0)) AS delivery_total
           FROM orders
           WHERE store_id = ? AND status = 'delivered'
           GROUP BY period
           ORDER BY period DESC`
        )
        .bind(store.id)
        .run();

      const walletResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', created_at) AS period,
                  SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS credits,
                  SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS debits
           FROM wallet_transactions
           WHERE driver_id IN (SELECT id FROM drivers WHERE store_id = ?)
           GROUP BY period
           ORDER BY period DESC`
        )
        .bind(store.id)
        .run();

      return jsonResponse(
        {
          ok: true,
          period: period ?? "daily",
          orders: ordersResult.results ?? [],
          wallet: walletResult.results ?? [],
        },
        200,
        origin
      );
    }

    if (request.method === "GET" && path === "/ledger/drivers") {
      const adminCode =
        getString(url.searchParams.get("admin_code")) ??
        getString(request.headers.get("x-admin-code"));
      const period = getString(url.searchParams.get("period"));
      const driverId = getString(url.searchParams.get("driver_id"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const store = await requireStore(env, null, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const format = periodExpr(period);
      const params: unknown[] = [store.id];
      let sql =
        `SELECT d.id AS driver_id,
                d.name AS driver_name,
                strftime('${format}', COALESCE(o.delivered_at, o.created_at)) AS period,
                COUNT(*) AS trips,
                SUM(COALESCE(o.delivery_fee, 0)) AS delivery_total
         FROM orders o
         JOIN drivers d ON o.driver_id = d.id
         WHERE d.store_id = ? AND o.status = 'delivered'`;

      if (driverId) {
        sql += " AND d.id = ?";
        params.push(driverId);
      }

      sql += " GROUP BY d.id, period ORDER BY period DESC, d.name ASC";

      const result = await env.nova_max_db.prepare(sql).bind(...params).run();

      return jsonResponse(
        {
          ok: true,
          period: period ?? "daily",
          drivers: result.results ?? [],
        },
        200,
        origin
      );
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
      const payoutMethod = getString(body?.payout_method) ?? "wallet";
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

      await broadcastEvent(env, effectiveStoreId, {
        type: "order_created",
        target_driver_id: driverId ?? undefined,
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
          created_at: new Date().toISOString(),
        },
      });

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

      const markDelivered =
        nextStatus === "delivered" && currentStatus !== "delivered";

      if (markDelivered) {
        await env.nova_max_db
          .prepare(
            "UPDATE orders SET status = ?, driver_id = ?, delivered_at = CURRENT_TIMESTAMP WHERE id = ?"
          )
          .bind(nextStatus, effectiveDriverId, orderId)
          .run();
      } else {
        await env.nova_max_db
          .prepare("UPDATE orders SET status = ?, driver_id = ? WHERE id = ?")
          .bind(nextStatus, effectiveDriverId, orderId)
          .run();
      }

      if (markDelivered && effectiveDriverId) {
        const fee = typeof order.delivery_fee === "number" ? order.delivery_fee : 0;
        const payoutMethod = order.payout_method ?? "wallet";

        if (fee > 0) {
          await env.nova_max_db
            .prepare(
              "UPDATE drivers SET wallet_balance = wallet_balance + ? WHERE id = ?"
            )
            .bind(fee, effectiveDriverId)
            .run();

          const txId = crypto.randomUUID();
          await env.nova_max_db
            .prepare(
              "INSERT INTO wallet_transactions (id, driver_id, amount, type, method, note) VALUES (?, ?, ?, ?, ?, ?)"
            )
            .bind(
              txId,
              effectiveDriverId,
              fee,
              "credit",
              payoutMethod,
              `Order ${orderId} delivered`
            )
            .run();
        }
      }

      if (order.store_id) {
        await broadcastEvent(env, order.store_id, {
          type: "order_status",
          order_id: orderId,
          status: nextStatus,
          driver_id: effectiveDriverId,
          target_driver_id: effectiveDriverId ?? undefined,
          delivered_at: markDelivered ? new Date().toISOString() : order.delivered_at,
        });
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
