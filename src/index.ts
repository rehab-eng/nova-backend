import * as Sentry from "@sentry/cloudflare";

export interface Env {
  nova_max_db: D1Database;
  REALTIME: DurableObjectNamespace;
  ALLOWED_ORIGIN?: string;
  SENTRY_DSN?: string;
  SENTRY_ENVIRONMENT?: string;
  ADMIN_MASTER_CODE?: string;
  DATA_SAFETY_LOCK?: string;
}

type OrderStatus = "pending" | "accepted" | "delivering" | "delivered" | "cancelled";

type StoreRow = {
  id: string;
  name: string;
  admin_code: string | null;
  store_code: string | null;
  wallet_balance: number | null;
};

type DriverRow = {
  id: string;
  name: string;
  phone: string | null;
  secret_code: string | null;
  status: string | null;
  wallet_balance: number | null;
  store_id: string | null;
  is_active: number | null;
};

type OrderRow = {
  id: string;
  store_id: string | null;
  store_name?: string | null;
  store_code?: string | null;
  driver_id: string | null;
  driver_name?: string | null;
  driver_phone?: string | null;
  customer_name: string | null;
  customer_phone: string | null;
  customer_location_text: string | null;
  order_type: string | null;
  receiver_name: string | null;
  payout_method: string | null;
  price: number | null;
  delivery_fee: number | null;
  cash_amount: number | null;
  wallet_amount: number | null;
  status: string | null;
  created_at: string | null;
  delivered_at: string | null;
  cancelled_at: string | null;
  cancel_reason: string | null;
  cancelled_by: string | null;
};

type WalletTxRow = {
  id: string;
  driver_id: string;
  amount: number;
  type: "credit" | "debit" | string;
  method: string | null;
  note: string | null;
  idempotency_key?: string | null;
  created_at: string | null;
};

type StoreWalletTxRow = {
  id: string;
  store_id: string;
  amount: number;
  type: "credit" | "debit" | string;
  method: string | null;
  note: string | null;
  idempotency_key?: string | null;
  related_driver_id?: string | null;
  related_order_id?: string | null;
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
  const requestOrigin = request.headers.get("Origin") ?? "*";
  const allowed = env.ALLOWED_ORIGIN;
  if (!allowed) return requestOrigin;
  const list = allowed
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  if (!list.length) return requestOrigin;
  if (list.includes("*")) return requestOrigin;
  if (list.includes(requestOrigin)) return requestOrigin;
  return requestOrigin;
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
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : null;
}

function getNormalized(value: unknown): string | null {
  return normalizeDigits(getString(value));
}

function normalizeAdminCode(value: unknown): string | null {
  const normalized = normalizeDigits(getString(value));
  return normalized ? normalized.toLowerCase() : null;
}

let cachedMasterCode: { value: string | null; ts: number } | null = null;
const MASTER_CACHE_TTL = 5 * 60 * 1000;

async function getMasterCode(env: Env): Promise<string | null> {
  const now = Date.now();
  if (cachedMasterCode && now - cachedMasterCode.ts < MASTER_CACHE_TTL) {
    return cachedMasterCode.value;
  }
  try {
    const row = await env.nova_max_db
      .prepare("SELECT value FROM system_settings WHERE key = 'master_code'")
      .first<{ value: string }>();
    const value = normalizeAdminCode(row?.value);
    if (value) {
      cachedMasterCode = { value, ts: now };
      return value;
    }
  } catch {
    // ignore
  }
  const fromEnv = normalizeAdminCode(env.ADMIN_MASTER_CODE);
  cachedMasterCode = { value: fromEnv ?? null, ts: now };
  return fromEnv ?? null;
}

function isMasterCode(adminCode: string | null, masterCode: string | null): boolean {
  if (!masterCode) return false;
  return normalizeAdminCode(adminCode) === masterCode;
}

function isDataSafetyLockOn(env: Env): boolean {
  const value = getString(env.DATA_SAFETY_LOCK);
  if (!value) return true;
  return value !== "0";
}


const ARABIC_DIGIT_MAP: Record<string, string> = {
  "\u0660": "0",
  "\u0661": "1",
  "\u0662": "2",
  "\u0663": "3",
  "\u0664": "4",
  "\u0665": "5",
  "\u0666": "6",
  "\u0667": "7",
  "\u0668": "8",
  "\u0669": "9",
  "\u06f0": "0",
  "\u06f1": "1",
  "\u06f2": "2",
  "\u06f3": "3",
  "\u06f4": "4",
  "\u06f5": "5",
  "\u06f6": "6",
  "\u06f7": "7",
  "\u06f8": "8",
  "\u06f9": "9",
};

function normalizeDigits(value: string | null): string | null {
  if (!value) return null;
  const normalized = value
    .replace(/[\u0660-\u0669\u06f0-\u06f9]/g, (digit) => ARABIC_DIGIT_MAP[digit] ?? digit)
    .trim();
  return normalized.length ? normalized : null;
}

function normalizeName(value: unknown): string | null {
  const raw = getString(value);
  if (!raw) return null;
  return raw
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
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

function getIdempotencyKey(value: unknown): string | null {
  const raw = getString(value);
  if (!raw) return null;
  return raw.normalize("NFKC").trim();
}

function isConstraintError(err: unknown): boolean {
  if (!err) return false;
  const message = err instanceof Error ? err.message : String(err);
  return (
    message.includes("UNIQUE constraint failed") ||
    message.includes("SQLITE_CONSTRAINT")
  );
}

function buildOrderNote(
  orderId: string,
  action: "delivered" | "accepted" | "cancelled",
  storeName?: string | null
): string {
  const base =
    action === "delivered"
      ? "تم التسليم"
      : action === "accepted"
        ? "تم القبول"
        : "تم الإلغاء";
  const suffix = storeName ? ` - ${storeName}` : "";
  return `طلب ${orderId} ${base}${suffix}`;
}

async function applyWalletTransaction(params: {
  env: Env;
  driverId: string;
  storeId?: string | null;
  amount: number;
  type: "credit" | "debit";
  method?: string | null;
  note?: string | null;
  idempotencyKey: string;
  relatedOrderId?: string | null;
}): Promise<
  | { ok: true; balance: number; storeBalance?: number | null }
  | { ok: false; error: string; duplicate?: boolean }
> {
  const { env, driverId, storeId, amount, type, method, note, idempotencyKey } = params;
  const driver = await env.nova_max_db
    .prepare("SELECT id, wallet_balance, store_id FROM drivers WHERE id = ?")
    .bind(driverId)
    .first<DriverRow>();
  if (!driver) return { ok: false, error: "Driver not found" };

  const currentDriver = typeof driver.wallet_balance === "number" ? driver.wallet_balance : 0;
  const delta = type === "credit" ? amount : -amount;
  const nextDriver = currentDriver + delta;
  if (nextDriver < 0) {
    return { ok: false, error: "Insufficient wallet balance" };
  }

  let storeBalance: number | null = null;
  let nextStore: number | null = null;
  let effectiveStoreId = storeId ?? driver.store_id ?? null;
  if (effectiveStoreId) {
    const store = await env.nova_max_db
      .prepare("SELECT id, wallet_balance FROM stores WHERE id = ?")
      .bind(effectiveStoreId)
      .first<StoreRow>();
    if (store) {
      storeBalance = typeof store.wallet_balance === "number" ? store.wallet_balance : 0;
      nextStore = storeBalance - delta;
    } else {
      effectiveStoreId = null;
    }
  }

  const driverTxId = crypto.randomUUID();
  const statements = [
    env.nova_max_db
      .prepare(
        "INSERT INTO wallet_transactions (id, driver_id, amount, type, method, note, idempotency_key) VALUES (?, ?, ?, ?, ?, ?, ?)"
      )
      .bind(driverTxId, driverId, amount, type, method ?? null, note ?? null, idempotencyKey),
    env.nova_max_db
      .prepare("UPDATE drivers SET wallet_balance = ? WHERE id = ?")
      .bind(nextDriver, driverId),
  ];

  if (effectiveStoreId && nextStore !== null) {
    const storeTxId = crypto.randomUUID();
    const storeType = type === "credit" ? "debit" : "credit";
    statements.push(
      env.nova_max_db
        .prepare(
          "INSERT INTO store_wallet_transactions (id, store_id, amount, type, method, note, idempotency_key, related_driver_id, related_order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(
          storeTxId,
          effectiveStoreId,
          amount,
          storeType,
          method ?? null,
          note ?? null,
          idempotencyKey,
          driverId,
          params.relatedOrderId ?? null
        )
    );
    statements.push(
      env.nova_max_db
        .prepare("UPDATE stores SET wallet_balance = ? WHERE id = ?")
        .bind(nextStore, effectiveStoreId)
    );
  }

  try {
    await env.nova_max_db.batch(statements);
  } catch (err) {
    if (isConstraintError(err)) {
      return { ok: false, error: "Database constraint error", duplicate: true };
    }
    throw err;
  }

  return { ok: true, balance: nextDriver, storeBalance: nextStore };
}


async function generateStoreCode(env: Env): Promise<string> {
  for (let i = 0; i < 8; i++) {
    const code = randomCode(4);
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
  const normalized = normalizeDigits(driverCode);
  if (!normalized) return null;
  return await env.nova_max_db
    .prepare(
      "SELECT * FROM drivers WHERE secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
    )
    .bind(normalized)
    .first<DriverRow>();
}

async function resolveDriverKey(env: Env, driverKey: string): Promise<DriverRow | null> {
  const normalized = normalizeDigits(driverKey) ?? driverKey;
  return await env.nova_max_db
    .prepare("SELECT * FROM drivers WHERE id = ? OR secret_code = ? OR phone = ?")
    .bind(normalized, normalized, normalized)
    .first<DriverRow>();
}

async function requireStore(
  env: Env,
  storeId: string | null,
  adminCode: string | null
): Promise<StoreRow | null> {
  const normalizedAdmin = normalizeDigits(adminCode);
  if (!normalizedAdmin || normalizedAdmin.length < 4) return null;
  const master = await getMasterCode(env);
  const masterOk = master ? normalizeAdminCode(normalizedAdmin) === master : false;
  let store: StoreRow | null = null;
  if (storeId) {
    if (masterOk) {
      store = await env.nova_max_db
        .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
        .bind(storeId, storeId)
        .first<StoreRow>();
    } else {
      store = await env.nova_max_db
        .prepare("SELECT * FROM stores WHERE (id = ? OR store_code = ?) AND admin_code = ?")
        .bind(storeId, storeId, normalizedAdmin)
        .first<StoreRow>();
    }
  } else {
    if (masterOk) return null;
    store = await env.nova_max_db
      .prepare("SELECT * FROM stores WHERE admin_code = ?")
      .bind(normalizedAdmin)
      .first<StoreRow>();
  }
  if (!store) return null;
  return await ensureStoreCode(env, store);
}

async function requireDriver(
  env: Env,
  driverId: string | null,
  secretCode: string | null
): Promise<DriverRow | null> {
  const normalized = normalizeDigits(secretCode);
  if (!driverId || !normalized) return null;
  return await env.nova_max_db
    .prepare(
      "SELECT * FROM drivers WHERE id = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
    )
    .bind(driverId, normalized)
    .first<DriverRow>();
}

async function listOrders(
  env: Env,
  filters: {
    storeId?: string | null;
    driverId?: string | null;
    status?: string | null;
    limit?: number | null;
    unassignedOnly?: boolean | null;
  }
): Promise<OrderRow[]> {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (filters.storeId) {
    clauses.push("o.store_id = ?");
    params.push(filters.storeId);
  }
  if (filters.driverId) {
    clauses.push("o.driver_id = ?");
    params.push(filters.driverId);
  }
  if (filters.status) {
    clauses.push("o.status = ?");
    params.push(filters.status);
  }
  if (filters.unassignedOnly) {
    clauses.push("o.driver_id IS NULL");
  }

  let sql =
    "SELECT o.*, s.name AS store_name, s.store_code AS store_code, d.name AS driver_name, d.phone AS driver_phone " +
    "FROM orders o " +
    "LEFT JOIN stores s ON o.store_id = s.id " +
    "LEFT JOIN drivers d ON o.driver_id = d.id";
  if (clauses.length) sql += " WHERE " + clauses.join(" AND ");
  sql += " ORDER BY o.created_at DESC LIMIT ?";

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

async function broadcastGlobalEvent(
  env: Env,
  event: Record<string, unknown>
): Promise<void> {
  try {
    const id = env.REALTIME.idFromName("global");
    const stub = env.REALTIME.get(id);
    await stub.fetch("https://realtime/event", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
    });
  } catch (err) {
    console.warn("Realtime global broadcast failed", err);
  }
}

function periodExpr(period: string | null): { format: string; modifier: string } {
  if (period === "yearly") return { format: "%Y", modifier: "+0 hours" };
  if (period === "monthly") return { format: "%Y-%m", modifier: "+0 hours" };
  if (period === "weekly") return { format: "%Y-%W", modifier: "+2 days" };
  return { format: "%Y-%m-%d", modifier: "+0 hours" };
}

type RealtimeRole = "admin" | "driver" | "store" | "guest";

type ConnectionMeta = {
  role: RealtimeRole;
  driverId: string | null;
};

export class RealtimeRoom {
  private state: DurableObjectState;
  private env: Env;
  private busyDrivers = new Set<string>();
  private offlineTimers = new Map<string, number>();

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async refreshDriverBusy(driverId: string): Promise<void> {
    const active = await this.env.nova_max_db
      .prepare(
        "SELECT id FROM orders WHERE driver_id = ? AND status IN ('accepted','delivering') LIMIT 1"
      )
      .bind(driverId)
      .first<{ id: string }>();

    if (active) {
      this.busyDrivers.add(driverId);
    } else {
      this.busyDrivers.delete(driverId);
    }
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
      roleHeader === "admin" ||
      roleHeader === "driver" ||
      roleHeader === "store"
        ? roleHeader
        : "guest";
    const driverId = request.headers.get("X-Driver-Id");

    const alreadyOnline = driverId ? this.hasDriverConnection(driverId) : false;
    if (driverId) {
      const pending = this.offlineTimers.get(driverId);
      if (pending) {
        clearTimeout(pending);
        this.offlineTimers.delete(driverId);
      }
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);
    const meta: ConnectionMeta = { role, driverId: driverId ?? null };
    server.serializeAttachment(meta);

    if (driverId && !alreadyOnline) {
      await this.setDriverStatus(driverId, "online");
    }
    if (driverId) {
      void this.refreshDriverBusy(driverId);
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
        attachment.role === "store" ||
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
    if (stillOnline) return;
    if (this.offlineTimers.has(meta.driverId)) return;
    const timer = setTimeout(async () => {
      const online = this.hasDriverConnection(meta.driverId);
      if (!online) {
        await this.setDriverStatus(meta.driverId, "offline");
        this.busyDrivers.delete(meta.driverId);
      }
      this.offlineTimers.delete(meta.driverId);
    }, 5000) as unknown as number;
    this.offlineTimers.set(meta.driverId, timer);
  }

  private async setDriverStatus(
    driverId: string,
    status: "online" | "offline"
  ): Promise<void> {
    await this.env.nova_max_db
      .prepare("UPDATE drivers SET status = ? WHERE id = ?")
      .bind(status, driverId)
      .run();

    const driver = await this.env.nova_max_db
      .prepare("SELECT store_id FROM drivers WHERE id = ?")
      .bind(driverId)
      .first<{ store_id: string | null }>();

    if (driver?.store_id) {
      await broadcastEvent(this.env, driver.store_id, {
        type: "driver_status",
        driver_id: driverId,
        status,
        ts: new Date().toISOString(),
        audience: "admin",
      });
    }

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
    const type = getString((event as { type?: unknown }).type);

    if (type === "order_status") {
      const driverId = getString((event as { driver_id?: unknown }).driver_id);
      const status = getString((event as { status?: unknown }).status);
      if (driverId) {
        if (status === "accepted" || status === "delivering") {
          this.busyDrivers.add(driverId);
        } else if (status === "delivered" || status === "cancelled") {
          this.busyDrivers.delete(driverId);
        }
      }
    }

    const filterBusyDrivers =
      type === "order_created" && audience === "driver" && !targetDriver;

    for (const socket of this.state.getWebSockets()) {
      const meta = this.getMeta(socket);
      if (audience === "admin" && meta.role !== "admin") continue;
      if (audience === "driver" && meta.role !== "driver") continue;
      if (
        filterBusyDrivers &&
        meta.role === "driver" &&
        meta.driverId &&
        this.busyDrivers.has(meta.driverId)
      ) {
        continue;
      }
      if (targetDriver && meta.role === "driver" && meta.driverId !== targetDriver) {
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

const handler = {
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

    if (request.method === "GET" && path === "/stores/public") {
      const result = await env.nova_max_db
        .prepare("SELECT id, name FROM stores ORDER BY name ASC")
        .run<StoreRow>();
      const stores = (result.results ?? []).map((store) => ({
        id: store.id,
        name: store.name ?? null,
      }));
      return jsonResponse({ ok: true, stores }, 200, origin);
    }

    if (request.method === "GET" && path === "/realtime") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return errorResponse("Expected websocket", 426, origin);
      }

      const role = getString(url.searchParams.get("role"));
      if (role !== "admin" && role !== "driver" && role !== "store") {
        return errorResponse("Invalid role", 400, origin);
      }

      let roomName: string | null = null;
      let driverId: string | null = null;

      if (role === "admin") {
        const adminCode =
          getNormalized(url.searchParams.get("admin_code")) ??
          getNormalized(request.headers.get("x-admin-code"));
        if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
        const storeKey =
          getString(url.searchParams.get("store_id")) ??
          getString(url.searchParams.get("store_code"));
        const masterCode = await getMasterCode(env);
        const masterEnabled = Boolean(masterCode);

        if (masterEnabled) {
          if (isMasterCode(adminCode, masterCode)) {
            if (storeKey) {
              const store = await requireStore(env, storeKey, adminCode);
              if (!store) return errorResponse("Store not found", 404, origin);
              roomName = store.id;
            } else {
              roomName = "global";
            }
          } else {
            const store = await requireStore(env, storeKey ?? null, adminCode);
            if (!store) return errorResponse("Unauthorized", 401, origin);
            roomName = store.id;
          }
        } else {
          const store = await requireStore(env, storeKey ?? null, adminCode);
          if (!store) return errorResponse("Unauthorized", 401, origin);
          roomName = store.id;
        }
      } else if (role === "store") {
        const storeIdParam = getString(url.searchParams.get("store_id"));
        const storeCode =
          getNormalized(url.searchParams.get("store_code")) ??
          getNormalized(url.searchParams.get("code")) ??
          getNormalized(url.searchParams.get("store"));
        const storeName =
          normalizeName(url.searchParams.get("store_name")) ??
          normalizeName(url.searchParams.get("name"));

        if (storeIdParam && !storeCode) {
          return errorResponse("Missing store_code", 400, origin);
        }

        const store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE store_code = ? OR id = ?")
          .bind(storeCode ?? storeIdParam ?? "", storeIdParam ?? storeCode ?? "")
          .first<StoreRow>();
        if (!store) return errorResponse("Store not found", 404, origin);

        if (storeIdParam) {
          if (!storeCode || normalizeDigits(store.store_code) !== storeCode) {
            return errorResponse("Unauthorized", 401, origin);
          }
        } else {
          if (!storeCode || !storeName) {
            return errorResponse("Missing store_code or store_name", 400, origin);
          }
          const normalizedName = normalizeName(store.name);
          if (!normalizedName || normalizedName !== storeName) {
            return errorResponse("Unauthorized", 401, origin);
          }
        }

        roomName = store.id;
      } else {
        const driverCode =
          getNormalized(url.searchParams.get("driver_code")) ??
          getNormalized(url.searchParams.get("secret_code")) ??
          getNormalized(request.headers.get("x-driver-code"));
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
            getNormalized(url.searchParams.get("secret_code")) ??
            getNormalized(request.headers.get("x-driver-code"));
          if (!secretCode) {
            return errorResponse("Missing driver_code", 400, origin);
          }
          driver = await requireDriver(env, driverIdParam, secretCode);
          if (!driver) return errorResponse("Unauthorized", 401, origin);
          driverId = driverIdParam;
        }

        roomName = "global";
      }

      if (!roomName) return errorResponse("Missing room", 400, origin);

      const id = env.REALTIME.idFromName(roomName);
      const stub = env.REALTIME.get(id);
      const headers = new Headers(request.headers);
      headers.set("X-Role", role);
      headers.set("X-Store-Id", roomName);
      if (driverId) headers.set("X-Driver-Id", driverId);

      const realtimeRequest = new Request("https://realtime/connect", {
        method: "GET",
        headers,
      });

      return stub.fetch(realtimeRequest);
    }

    if (request.method === "POST" && path === "/stores") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      if (masterCode && !isMasterCode(adminCode, masterCode)) {
        return errorResponse("Unauthorized", 401, origin);
      }
      const name = getString(body?.name);
      if (!name) return errorResponse("Missing store name", 400, origin);

      const id = crypto.randomUUID();
      const storeAdminCode = randomCode(8);
      const storeCode = await generateStoreCode(env);

      await env.nova_max_db
        .prepare("INSERT INTO stores (id, name, admin_code, store_code) VALUES (?, ?, ?, ?)")
        .bind(id, name, storeAdminCode, storeCode)
        .run();

      return jsonResponse(
        { ok: true, store: { id, name, admin_code: storeAdminCode, store_code: storeCode } },
        201,
        origin
      );
    }

    if (request.method === "GET" && path === "/stores") {
      const adminCode =
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      if (masterCode && !isMasterCode(adminCode, masterCode)) {
        return errorResponse("Unauthorized", 401, origin);
      }

      const result = await env.nova_max_db
        .prepare("SELECT id, name, admin_code, store_code FROM stores ORDER BY name ASC")
        .run<StoreRow>();

      return jsonResponse({ ok: true, stores: result.results ?? [] }, 200, origin);
    }

    if (request.method === "GET" && path === "/stores/track") {
      const storeIdParam = getString(url.searchParams.get("store_id"));
      const storeCode =
        getNormalized(url.searchParams.get("store_code")) ??
        getNormalized(url.searchParams.get("code")) ??
        getNormalized(url.searchParams.get("store"));
      const storeName =
        normalizeName(url.searchParams.get("store_name")) ??
        normalizeName(url.searchParams.get("name"));

      if (!storeIdParam && (!storeCode || !storeName)) {
        return errorResponse("Missing store_id or store_code", 400, origin);
      }
      if (storeIdParam && !storeCode) {
        return errorResponse("Missing store_code", 400, origin);
      }

      const store = await env.nova_max_db
        .prepare(
          "SELECT id, name, store_code FROM stores WHERE id = ? OR store_code = ?"
        )
        .bind(storeIdParam ?? storeCode ?? "", storeCode ?? storeIdParam ?? "")
        .first<StoreRow>();

      if (!store) return errorResponse("Store not found", 404, origin);

      if (storeIdParam) {
        if (!storeCode || normalizeDigits(store.store_code) !== storeCode) {
          return errorResponse("Unauthorized", 401, origin);
        }
      } else {
        const normalizedName = normalizeName(store.name);
        if (!normalizedName || normalizedName !== storeName) {
          return errorResponse("Unauthorized", 401, origin);
        }
      }

      const ensured = await ensureStoreCode(env, store);
      const orders = await listOrders(env, { storeId: ensured.id, limit: 200 });
      const slim = orders.map((order) => ({
        id: order.id,
        status: order.status ?? null,
        created_at: order.created_at ?? null,
        delivered_at: order.delivered_at ?? null,
        cancelled_at: order.cancelled_at ?? null,
        customer_name: order.customer_name ?? null,
        customer_phone: order.customer_phone ?? null,
        receiver_name: order.receiver_name ?? null,
        order_type: order.order_type ?? null,
        cancel_reason: order.cancel_reason ?? null,
        cancelled_by: order.cancelled_by ?? null,
      }));

      return jsonResponse(
        {
          ok: true,
          store: {
            id: ensured.id,
            name: ensured.name,
            store_code: ensured.store_code,
          },
          orders: slim,
        },
        200,
        origin
      );
    }

    if (request.method === "POST" && path === "/stores/by-admin") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);

      let store: StoreRow | null = null;
      const masterCode = await getMasterCode(env);
      if (masterCode) {
        if (!isMasterCode(adminCode, masterCode)) {
          return errorResponse("Unauthorized", 401, origin);
        }
        const storeKey = getString(body?.store_id) ?? getString(body?.store_code);
        if (!storeKey) return errorResponse("Missing store_id", 400, origin);
        store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
          .bind(storeKey, storeKey)
          .first<StoreRow>();
      } else {
        store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE admin_code = ?")
          .bind(adminCode)
          .first<StoreRow>();
      }

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
    }

    if (request.method === "DELETE" && segments[0] === "stores" && segments.length === 2) {
      const storeKey = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code")) ??
        getNormalized(url.searchParams.get("admin_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      if (masterCode && !isMasterCode(adminCode, masterCode)) {
        return errorResponse("Unauthorized", 401, origin);
      }

      const masterEnabled = Boolean(masterCode);
      const store = await env.nova_max_db
        .prepare(
          masterEnabled
            ? "SELECT * FROM stores WHERE id = ? OR store_code = ?"
            : "SELECT * FROM stores WHERE (id = ? OR store_code = ?) AND admin_code = ?"
        )
        .bind(
          masterEnabled ? storeKey : storeKey,
          masterEnabled ? storeKey : storeKey,
          ...(masterEnabled ? [] : [adminCode])
        )
        .first<StoreRow>();

      if (!store) return errorResponse("Store not found", 404, origin);

      const purge = getString(url.searchParams.get("purge")) ?? getString(body?.purge);
      const shouldPurge = purge === "1" || purge === "true";
      if (!shouldPurge) {
        return errorResponse("purge=1 required", 400, origin);
      }
      if (isDataSafetyLockOn(env)) {
        return errorResponse("Data safety lock enabled", 403, origin);
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

    if (request.method === "POST" && path === "/drivers") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      const storeId =
        getString(body?.store_id) ?? getString(body?.store_code);
      const name = getString(body?.name);
      const phone = normalizeDigits(getString(body?.phone));

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!name || !phone) {
        return errorResponse("Missing driver name or phone", 400, origin);
      }

      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      const masterOk = isMasterCode(adminCode, masterCode);
      let store: StoreRow | null = null;

      if (masterEnabled && masterOk) {
        if (storeId) {
          store = await env.nova_max_db
            .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
            .bind(storeId, storeId)
            .first<StoreRow>();
          if (!store) return errorResponse("Store not found", 404, origin);
        }
      } else {
        store = await requireStore(env, storeId ?? null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

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
            "INSERT INTO drivers (id, name, phone, secret_code, store_id, is_active) VALUES (?, ?, ?, ?, ?, 1)"
          )
          .bind(id, name, phone, secretCode, store?.id ?? null)
          .run();

        if (store?.id) {
          await broadcastEvent(env, store.id, {
            type: "driver_created",
            driver: {
              id,
              name,
              phone,
              store_id: store.id,
              status: "offline",
              is_active: 1,
              driver_code: secretCode,
            },
            audience: "admin",
          });
        }

        await broadcastGlobalEvent(env, {
          type: "driver_created",
          driver: {
            id,
            name,
            phone,
            store_id: store?.id ?? null,
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
              store_id: store?.id ?? null,
              driver_code: secretCode,
              secret_code: secretCode,
            },
          },
          201,
          origin
        );
      } catch {
        return errorResponse("Could not allocate a unique driver code", 500, origin);
      }
    }

    if (request.method === "GET" && path === "/drivers/search") {
      const adminCode =
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      const query = getNormalized(url.searchParams.get("query"));
      const onlineParam = getString(url.searchParams.get("online"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      const masterOk = isMasterCode(adminCode, masterCode);
      let store: StoreRow | null = null;
      if (masterEnabled) {
        if (!masterOk) {
          store = await requireStore(env, null, adminCode);
          if (!store) return errorResponse("Unauthorized", 401, origin);
        }
      } else {
        store = await requireStore(env, null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      if (!query) return jsonResponse({ ok: true, drivers: [] }, 200, origin);

      const like = `%${query}%`;
      let sql =
        "SELECT id, name, phone, status, is_active, secret_code FROM drivers WHERE (name LIKE ? OR phone LIKE ? OR secret_code LIKE ?)";
      const params: unknown[] = [like, like, like];
      if (store) {
        sql += " AND store_id = ?";
        params.push(store.id);
      }
      if (onlineParam === "1" || onlineParam === "true") {
        sql += " AND status = 'online' AND (is_active = 1 OR is_active IS NULL)";
      }
      sql += " ORDER BY status DESC, name ASC LIMIT 25";

      const result = await env.nova_max_db
        .prepare(sql)
        .bind(...params)
        .run<DriverRow>();

      const drivers = (result.results ?? []).map((driver) => {
        const { secret_code, ...rest } = driver as any;
        return { ...rest, driver_code: secret_code ?? null };
      });

      return jsonResponse({ ok: true, drivers }, 200, origin);
    }

    if (request.method === "GET" && path === "/drivers") {
      const adminCode =
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      const activeParam = getString(url.searchParams.get("active"));
      const limit = parseNumber(url.searchParams.get("limit"));
      const storeKey =
        getString(url.searchParams.get("store_id")) ??
        getString(url.searchParams.get("store_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      const masterOk = isMasterCode(adminCode, masterCode);
      let store: StoreRow | null = null;

      if (masterEnabled && masterOk) {
        if (storeKey) {
          store = await env.nova_max_db
            .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
            .bind(storeKey, storeKey)
            .first<StoreRow>();
          if (!store) return errorResponse("Store not found", 404, origin);
        }
      } else {
        store = await requireStore(env, storeKey ?? null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      const clauses: string[] = [];
      const params: unknown[] = [];

      if (store) {
        clauses.push("store_id = ?");
        params.push(store.id);
      }
      if (activeParam !== "all") {
        if (activeParam === "0" || activeParam === "false") {
          clauses.push("is_active = 0");
        } else {
          clauses.push("(is_active = 1 OR is_active IS NULL)");
        }
      }

      let sql =
        "SELECT id, name, phone, status, wallet_balance, store_id, is_active, secret_code FROM drivers";
      if (clauses.length) sql += " WHERE " + clauses.join(" AND ");
      sql += " ORDER BY name ASC";

      const safeLimit = Math.min(Math.max(limit ?? 200, 1), 500);
      sql += " LIMIT ?";

      const result = await env.nova_max_db
        .prepare(sql)
        .bind(...params, safeLimit)
        .run<DriverRow>();

      const drivers = (result.results ?? []).map((driver) => {
        const { secret_code, ...rest } = driver as any;
        return { ...rest, driver_code: secret_code ?? null };
      });

      return jsonResponse({ ok: true, drivers }, 200, origin);
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "active"
    ) {
      const driverKey = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code")) ??
        getNormalized(url.searchParams.get("admin_code"));
      const activeNum = parseNumber(body?.active);
      const activeBool =
        typeof body?.active === "boolean"
          ? body.active
          : activeNum !== null
          ? activeNum !== 0
          : null;

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      if (activeBool === null) return errorResponse("Missing active flag", 400, origin);

      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      const masterOk = isMasterCode(adminCode, masterCode);
      let store: StoreRow | null = null;
      if (masterEnabled) {
        if (!masterOk) {
          store = await requireStore(env, null, adminCode);
          if (!store) return errorResponse("Unauthorized", 401, origin);
        }
      } else {
        store = await requireStore(env, null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      const existing = await resolveDriverKey(env, driverKey);
      if (!existing) return errorResponse("Driver not found", 404, origin);
      const driverId = existing.id;

      if (store && existing.store_id && existing.store_id !== store.id) {
        return errorResponse("Unauthorized", 401, origin);
      }

      await env.nova_max_db
        .prepare(
          "UPDATE drivers SET is_active = ?, status = ? WHERE id = ?"
        )
        .bind(activeBool ? 1 : 0, activeBool ? "offline" : "offline", driverId)
        .run();

      const eventPayload = {
        type: activeBool ? "driver_active" : "driver_disabled",
        driver_id: driverId,
        ts: new Date().toISOString(),
        audience: "admin",
      };

      if (store?.id) {
        await broadcastEvent(env, store.id, eventPayload);
      }
      await broadcastGlobalEvent(env, eventPayload);

      return jsonResponse(
        { ok: true, driver_id: driverId, active: activeBool },
        200,
        origin
      );
    }


    if (request.method === "POST" && path === "/drivers/login") {
      const body = await request.json().catch(() => null);
      const phone = normalizeDigits(getString(body?.phone));
      const secretCode = normalizeDigits(
        getString(body?.secret_code) ?? getString(body?.driver_code)
      );

      if (!phone || !secretCode) {
        return errorResponse("Missing phone or driver_code", 400, origin);
      }

      const driver = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, status, wallet_balance, store_id, is_active, secret_code FROM drivers WHERE phone = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
        )
        .bind(phone, secretCode)
        .first<DriverRow>();

      if (!driver) return errorResponse("Invalid credentials", 401, origin);

      const { secret_code, ...rest } = driver as any;
      return jsonResponse(
        { ok: true, driver: { ...rest, driver_code: secret_code ?? null } },
        200,
        origin
      );
    }

    if (
      request.method === "PATCH" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "status"
    ) {
      const driverKey = segments[1];
      const body = await request.json().catch(() => null);
      const secretCode =
        getNormalized(body?.secret_code) ??
        getNormalized(body?.driver_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const status = getString(body?.status);

      if (!secretCode || !status) {
        return errorResponse("Missing secret_code or status", 400, origin);
      }
      if (status !== "online" && status !== "offline") {
        return errorResponse("Invalid status", 400, origin);
      }

      const existing = await resolveDriverKey(env, driverKey);
      if (!existing) return errorResponse("Driver not found", 404, origin);
      const driverId = existing.id;

      const driver = await requireDriver(env, driverId, secretCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      await env.nova_max_db
        .prepare("UPDATE drivers SET status = ? WHERE id = ?")
        .bind(status, driverId)
        .run();

      const statusEvent = {
        type: "driver_status",
        driver_id: driverId,
        status,
        ts: new Date().toISOString(),
        audience: "admin",
      };

      if (driver.store_id) {
        await broadcastEvent(env, driver.store_id, statusEvent);
      }
      await broadcastGlobalEvent(env, statusEvent);

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
        getNormalized(body?.secret_code) ??
        getNormalized(body?.driver_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const name = getString(body?.name);

      if (!secretCode) return errorResponse("Missing secret_code", 400, origin);
      if (!name) {
        return errorResponse("Missing profile data", 400, origin);
      }

      const driver = await requireDriver(env, driverId, secretCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      const fields: string[] = [];
      const params: unknown[] = [];
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
          "SELECT id, name, phone, status, wallet_balance, store_id, is_active FROM drivers WHERE id = ?"
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
        getNormalized(url.searchParams.get("secret_code")) ??
        getNormalized(request.headers.get("x-driver-code"));
      if (!secretCode) return errorResponse("Missing secret_code", 400, origin);

      const driver = await env.nova_max_db
        .prepare(
          "SELECT id, name, phone, status, wallet_balance, store_id, is_active FROM drivers WHERE id = ? AND secret_code = ? AND (is_active = 1 OR is_active IS NULL)"
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

      const existing = await resolveDriverKey(env, driverKey);
      if (!existing) return errorResponse("Driver not found", 404, origin);

      const purge = getString(url.searchParams.get("purge")) ?? getString(body?.purge);
      const shouldPurge = purge === "1" || purge === "true";

      if (shouldPurge) {
        if (isDataSafetyLockOn(env)) {
          return errorResponse("Data safety lock enabled", 403, origin);
        }
        await env.nova_max_db.batch([
          env.nova_max_db.prepare("DELETE FROM wallet_transactions WHERE driver_id = ?").bind(driverId),
          env.nova_max_db.prepare("DELETE FROM orders WHERE driver_id = ?").bind(driverId),
          env.nova_max_db.prepare("DELETE FROM drivers WHERE id = ?").bind(driverId),
        ]);

        return jsonResponse({ ok: true, purged: true }, 200, origin);
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
      const driverKey = segments[1];
      const body = await request.json().catch(() => null);
      const adminCode =
        getString(body?.admin_code) ?? getString(request.headers.get("x-admin-code"));
      const amount = parseNumber(body?.amount);
      const method = getString(body?.method);
      const note = getString(body?.note);
      const type = segments[3] === "credit" ? "credit" : "debit";
      const idempotencyKey =
        getIdempotencyKey(body?.idempotency_key) ?? `manual:${crypto.randomUUID()}`;
      const storeKey = getString(body?.store_id) ?? getString(body?.store_code);
      const masterCode = await getMasterCode(env);
      const masterOk = isMasterCode(adminCode, masterCode);

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      if (!amount || amount <= 0) {
        return errorResponse("Invalid amount", 400, origin);
      }

      const driver = await resolveDriverKey(env, driverKey);
      if (!driver) return errorResponse("Driver not found", 404, origin);

      let store: StoreRow | null = null;
      if (masterOk) {
        const effectiveStoreKey = storeKey ?? driver.store_id ?? null;
        if (!effectiveStoreKey) {
          return errorResponse("store_id required", 400, origin);
        }
        store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
          .bind(effectiveStoreKey, effectiveStoreKey)
          .first<StoreRow>();
        if (!store) return errorResponse("Store not found", 404, origin);
      } else {
        store = await requireStore(env, storeKey ?? null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      if (driver.store_id && store && driver.store_id !== store.id) {
        return errorResponse("Driver not in store", 409, origin);
      }

      const result = await applyWalletTransaction({
        env,
        driverId: driver.id,
        storeId: store.id,
        amount,
        type,
        method,
        note,
        idempotencyKey,
      });
      if (!result.ok) {
        return errorResponse(
          result.error,
          result.duplicate ? 409 : 400,
          origin
        );
      }

      const broadcastStoreId = driver.store_id ?? store.id;
      if (broadcastStoreId) {
        await broadcastEvent(env, broadcastStoreId, {
          type: "wallet_transaction",
          driver_id: driver.id,
          balance: result.balance,
          target_driver_id: driver.id,
          transaction: {
            id: idempotencyKey,
            amount,
            type,
            method,
            note,
            created_at: new Date().toISOString(),
          },
        });
      }

      return jsonResponse({ ok: true, balance: result.balance }, 200, origin);
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
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      const secretCode =
        getNormalized(url.searchParams.get("secret_code")) ??
        getNormalized(request.headers.get("x-driver-code"));
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

    if (
      request.method === "GET" &&
      segments[0] === "drivers" &&
      segments.length === 3 &&
      segments[2] === "ledger"
    ) {
      const driverId = segments[1];
      const period = getString(url.searchParams.get("period"));
      const driverCode =
        getNormalized(url.searchParams.get("driver_code")) ??
        getNormalized(url.searchParams.get("secret_code")) ??
        getNormalized(request.headers.get("x-driver-code"));
      if (!driverCode) return errorResponse("Missing driver_code", 400, origin);

      const driver = await requireDriver(env, driverId, driverCode);
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      const { format, modifier } = periodExpr(period);

      const ordersResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', datetime(COALESCE(delivered_at, created_at), '${modifier}')) AS period,
                  COUNT(*) AS trips,
                  SUM(COALESCE(delivery_fee, 0)) AS delivery_total,
                  SUM(COALESCE(cash_amount, 0)) AS cash_total,
                  SUM(COALESCE(wallet_amount, 0)) AS wallet_total
           FROM orders
           WHERE driver_id = ? AND status = 'delivered'
           GROUP BY period
           ORDER BY period DESC`
        )
        .bind(driverId)
        .run();

      const walletResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', datetime(created_at, '${modifier}')) AS period,
                  SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS credits,
                  SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS debits
           FROM wallet_transactions
           WHERE driver_id = ?
           GROUP BY period
           ORDER BY period DESC`
        )
        .bind(driverId)
        .run<WalletTxRow>();

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

    if (request.method === "GET" && path === "/ledger/summary") {
      const adminCode =
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      const period = getString(url.searchParams.get("period"));
      const storeKey =
        getString(url.searchParams.get("store_id")) ??
        getString(url.searchParams.get("store_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      let store: StoreRow | null = null;
      if (masterEnabled && isMasterCode(adminCode, masterCode)) {
        if (!storeKey) {
          return errorResponse("store_id required", 400, origin);
        }
        store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
          .bind(storeKey, storeKey)
          .first<StoreRow>();
        if (!store) return errorResponse("Store not found", 404, origin);
      } else {
        store = await requireStore(env, storeKey ?? null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      const { format, modifier } = periodExpr(period);

      const ordersResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', datetime(COALESCE(delivered_at, created_at), '${modifier}')) AS period,
                  COUNT(*) AS trips,
                  SUM(COALESCE(delivery_fee, 0)) AS delivery_total,
                  SUM(COALESCE(cash_amount, 0)) AS cash_total,
                  SUM(COALESCE(wallet_amount, 0)) AS wallet_total
           FROM orders
           WHERE store_id = ? AND status = 'delivered'
           GROUP BY period
           ORDER BY period DESC`
        )
        .bind(store.id)
        .run();

      const walletResult = await env.nova_max_db
        .prepare(
          `SELECT strftime('${format}', datetime(created_at, '${modifier}')) AS period,
                  SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS credits,
                  SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS debits
           FROM store_wallet_transactions
           WHERE store_id = ?
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
        getNormalized(url.searchParams.get("admin_code")) ??
        getNormalized(request.headers.get("x-admin-code"));
      const period = getString(url.searchParams.get("period"));
      const driverId = getString(url.searchParams.get("driver_id"));
      const storeKey =
        getString(url.searchParams.get("store_id")) ??
        getString(url.searchParams.get("store_code"));

      if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
      const masterCode = await getMasterCode(env);
      const masterEnabled = Boolean(masterCode);
      let store: StoreRow | null = null;
      if (masterEnabled && isMasterCode(adminCode, masterCode)) {
        if (!storeKey) {
          return errorResponse("store_id required", 400, origin);
        }
        store = await env.nova_max_db
          .prepare("SELECT * FROM stores WHERE id = ? OR store_code = ?")
          .bind(storeKey, storeKey)
          .first<StoreRow>();
        if (!store) return errorResponse("Store not found", 404, origin);
      } else {
        store = await requireStore(env, storeKey ?? null, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
      }

      const { format, modifier } = periodExpr(period);
      const params: unknown[] = [store.id];
      let sql =
        `SELECT d.id AS driver_id,
                d.name AS driver_name,
                strftime('${format}', datetime(COALESCE(o.delivered_at, o.created_at), '${modifier}')) AS period,
                COUNT(*) AS trips,
                SUM(COALESCE(o.delivery_fee, 0)) AS delivery_total,
                SUM(COALESCE(o.cash_amount, 0)) AS cash_total,
                SUM(COALESCE(o.wallet_amount, 0)) AS wallet_total
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
      const driverCode =
        getNormalized(url.searchParams.get("driver_code")) ??
        getNormalized(request.headers.get("x-driver-code"));

      if (storeId) {
        if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
        const store = await requireStore(env, storeId, getNormalized(adminCode));
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      }

      if (!storeId && adminCode) {
        const masterCode = await getMasterCode(env);
        if (masterCode) {
          return errorResponse("store_id required", 400, origin);
        }
        const store = await requireStore(env, null, getNormalized(adminCode));
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      }

      if (driverId) {
        if (!driverCode) return errorResponse("Missing driver_code", 400, origin);
        const driver = await requireDriver(env, driverId, driverCode);
        if (!driver) return errorResponse("Unauthorized", 401, origin);
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
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      const storeId = getString(body?.store_id);
      const driverId = getString(body?.driver_id);
      const driverCode =
        getNormalized(body?.driver_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const customerName = getString(body?.customer_name);
      const customerPhone =
        getNormalized(body?.customer_phone) ?? getNormalized(body?.phone);
      const customerLocation = getString(body?.customer_location_text);
      const orderType = getString(body?.order_type);
      const receiverName = getString(body?.receiver_name);
      const payoutMethod = "cash";
      const price = parseNumber(body?.price);
      const deliveryFee = parseNumber(body?.delivery_fee);

      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }
      if (!customerName || !customerLocation || !customerPhone) {
        return errorResponse(
          "Missing customer_name, customer_phone, or customer_location_text",
          400,
          origin
        );
      }

      const store = await requireStore(env, storeId, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);
      const effectiveStoreId = store.id;
      const storeName = store.name;
      const storeCode = store.store_code ?? null;
      let assignedDriverId = driverId;
      if (driverCode && !assignedDriverId) {
        const driverByCode = await requireDriverByCode(env, driverCode);
        if (!driverByCode) return errorResponse("Driver not found", 404, origin);
        assignedDriverId = driverByCode.id;
      }
      const safePrice = typeof price === "number" ? price : 0;
      const cashAmount = payoutMethod === "cash" ? safePrice : 0;
      const walletAmount = payoutMethod === "cash" ? 0 : safePrice;

      const orderId = crypto.randomUUID();
      const ts = new Date().toISOString();
      const orderPayload = {
        id: orderId,
        store_id: effectiveStoreId,
        store_name: storeName,
        store_code: storeCode,
        driver_id: assignedDriverId,
        customer_name: customerName,
        customer_phone: customerPhone,
        customer_location_text: customerLocation,
        order_type: orderType,
        receiver_name: receiverName,
        payout_method: payoutMethod,
        price,
        delivery_fee: deliveryFee,
        cash_amount: cashAmount,
        wallet_amount: walletAmount,
        status: "pending",
        created_at: ts,
      };

      await env.nova_max_db
        .prepare(
          "INSERT INTO orders (id, store_id, driver_id, customer_name, customer_phone, customer_location_text, order_type, receiver_name, payout_method, price, delivery_fee, cash_amount, wallet_amount, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(
          orderId,
          effectiveStoreId,
          assignedDriverId,
          customerName,
          customerPhone,
          customerLocation,
          orderType,
          receiverName,
          payoutMethod,
          price,
          deliveryFee,
          cashAmount,
          walletAmount,
          "pending"
        )
        .run();

      await broadcastEvent(env, effectiveStoreId, {
        type: "order_created",
        target_driver_id: assignedDriverId ?? undefined,
        ts,
        order: orderPayload,
      });

      await broadcastGlobalEvent(env, {
        type: "order_created",
        audience: "admin",
        target_driver_id: assignedDriverId ?? undefined,
        ts,
        order: orderPayload,
      });

      await broadcastGlobalEvent(env, {
        type: "order_created",
        audience: "driver",
        target_driver_id: assignedDriverId ?? undefined,
        ts,
        order: orderPayload,
      });

      return jsonResponse(
        {
          ok: true,
          order: {
            id: orderId,
            store_id: effectiveStoreId,
            store_name: storeName,
            store_code: storeCode,
            driver_id: assignedDriverId,
            customer_name: customerName,
            customer_phone: customerPhone,
            customer_location_text: customerLocation,
            order_type: orderType,
            receiver_name: receiverName,
            payout_method: payoutMethod,
            price,
            delivery_fee: deliveryFee,
            cash_amount: cashAmount,
            wallet_amount: walletAmount,
            status: "pending",
          },
        },
        201,
        origin
      );
    }

    if (request.method === "POST" && path === "/orders/bulk") {
      const body = await request.json().catch(() => null);
      const adminCode =
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      if (!adminCode) {
        return errorResponse("Missing admin_code", 400, origin);
      }

      const storeKey = getString(body?.store_id) ?? getString(body?.store_code);
      const store = await requireStore(env, storeKey, adminCode);
      if (!store) return errorResponse("Unauthorized", 401, origin);

      const ordersInput = Array.isArray(body?.orders) ? body.orders : null;
      if (!ordersInput || ordersInput.length === 0) {
        return errorResponse("Missing orders array", 400, origin);
      }
      if (ordersInput.length > 1000) {
        return errorResponse("Too many orders", 413, origin);
      }

      const statements = [];
      const created: Array<Record<string, unknown>> = [];
      const now = new Date().toISOString();

      for (let i = 0; i < ordersInput.length; i += 1) {
        const item = ordersInput[i] ?? {};
        const customerName = getString(item?.customer_name);
        const customerPhone =
          getNormalized(item?.customer_phone) ?? getNormalized(item?.phone);
        const customerLocation = getString(item?.customer_location_text);
        const orderType = getString(item?.order_type);
        const receiverName = getString(item?.receiver_name);
        const payoutMethod = "cash";
        const price = parseNumber(item?.price);
        const deliveryFee = parseNumber(item?.delivery_fee);
        const driverCode = getNormalized(item?.driver_code);
        const driverId = getString(item?.driver_id);

        if (!customerName || !customerPhone || !customerLocation) {
          return errorResponse(`Invalid order at index ${i}`, 400, origin);
        }

        let assignedDriverId = driverId;
        if (driverCode && !assignedDriverId) {
          const driverByCode = await requireDriverByCode(env, driverCode);
          if (!driverByCode) {
            return errorResponse(`Driver not found at index ${i}`, 404, origin);
          }
          assignedDriverId = driverByCode.id;
        }

        const safePrice = typeof price === "number" ? price : 0;
        const cashAmount = payoutMethod === "cash" ? safePrice : 0;
        const walletAmount = payoutMethod === "cash" ? 0 : safePrice;
        const orderId = crypto.randomUUID();

        created.push({
          id: orderId,
          store_id: store.id,
          store_name: store.name,
          store_code: store.store_code ?? null,
          driver_id: assignedDriverId,
          customer_name: customerName,
          customer_phone: customerPhone,
          customer_location_text: customerLocation,
          order_type: orderType,
          receiver_name: receiverName,
          payout_method: payoutMethod,
          price,
          delivery_fee: deliveryFee,
          cash_amount: cashAmount,
          wallet_amount: walletAmount,
          status: "pending",
          created_at: now,
        });

        statements.push(
          env.nova_max_db
            .prepare(
              "INSERT INTO orders (id, store_id, driver_id, customer_name, customer_phone, customer_location_text, order_type, receiver_name, payout_method, price, delivery_fee, cash_amount, wallet_amount, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(
              orderId,
              store.id,
              assignedDriverId,
              customerName,
              customerPhone,
              customerLocation,
              orderType,
              receiverName,
              payoutMethod,
              price,
              deliveryFee,
              cashAmount,
              walletAmount,
              "pending"
            )
        );
      }

      await env.nova_max_db.batch(statements);

      for (const order of created) {
        await broadcastEvent(env, store.id, {
          type: "order_created",
          target_driver_id: (order as { driver_id?: string | null }).driver_id ?? undefined,
          ts: now,
          order,
        });
        await broadcastGlobalEvent(env, {
          type: "order_created",
          audience: "admin",
          ts: now,
          order,
        });
        await broadcastGlobalEvent(env, {
          type: "order_created",
          audience: "driver",
          ts: now,
          order,
        });
      }

      return jsonResponse({ ok: true, count: created.length, orders: created }, 201, origin);
    }

    if (request.method === "GET" && path === "/orders") {
      let storeId = getString(url.searchParams.get("store_id"));
      const adminCode = getNormalized(url.searchParams.get("admin_code"));
      const driverId = getString(url.searchParams.get("driver_id"));
      const status = getString(url.searchParams.get("status"));
      const limit = parseNumber(url.searchParams.get("limit"));
      const driverCode =
        getNormalized(url.searchParams.get("driver_code")) ??
        getNormalized(request.headers.get("x-driver-code"));
      const masterCode = await getMasterCode(env);
      const masterOk = isMasterCode(adminCode, masterCode);

      if (storeId) {
        if (!adminCode) return errorResponse("Missing admin_code", 400, origin);
        const store = await requireStore(env, storeId, adminCode);
        if (!store) return errorResponse("Unauthorized", 401, origin);
        storeId = store.id;
      }

      if (!storeId && adminCode) {
        if (masterCode && !masterOk) {
          return errorResponse("store_id required", 400, origin);
        }
        if (!masterOk) {
          const store = await requireStore(env, null, adminCode);
          if (!store) return errorResponse("Unauthorized", 401, origin);
          storeId = store.id;
        }
      }

      if (driverId) {
        if (!driverCode && !masterOk) {
          return errorResponse("Missing driver_code", 400, origin);
        }
        if (!masterOk) {
          const driver = await requireDriver(env, driverId, driverCode);
          if (!driver) return errorResponse("Unauthorized", 401, origin);
        } else {
          const driver = await env.nova_max_db
            .prepare("SELECT id FROM drivers WHERE id = ?")
            .bind(driverId)
            .first<{ id: string }>();
          if (!driver) return errorResponse("Driver not found", 404, origin);
        }
      }

      if (!storeId && !adminCode && !driverId) {
        if (!driverCode) {
          return errorResponse("store_id or driver_id required", 400, origin);
        }
        const driver = await requireDriverByCode(env, driverCode);
        if (!driver) return errorResponse("Unauthorized", 401, origin);
        if (status && status !== "pending") {
          return errorResponse("Invalid status", 400, origin);
        }
        const orders = await listOrders(env, {
          status: "pending",
          limit,
          unassignedOnly: true,
        });
        return jsonResponse({ ok: true, orders }, 200, origin);
      }

      const orders = await listOrders(env, { storeId, driverId, status, limit });
      return jsonResponse({ ok: true, orders }, 200, origin);
    }

    if (
      request.method === "POST" &&
      segments[0] === "orders" &&
      segments.length === 3 &&
      segments[2] === "decline"
    ) {
      const orderId = segments[1];
      const body = await request.json().catch(() => null);
      const driverId =
        getString(body?.driver_id) ?? getString(request.headers.get("x-driver-id"));
      const driverCode =
        getNormalized(body?.driver_code) ??
        getNormalized(body?.secret_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const cancelReason =
        getString(body?.cancel_reason) ??
        getString(body?.reason) ??
        getString(body?.note);
      if (!driverCode) return errorResponse("Missing driver_code", 400, origin);
      if (!cancelReason) return errorResponse("Missing cancel_reason", 400, origin);

      let driver: DriverRow | null = null;
      if (driverId) {
        driver = await requireDriver(env, driverId, driverCode);
      }
      if (!driver) {
        driver = await requireDriverByCode(env, driverCode);
      }
      if (!driver) return errorResponse("Unauthorized", 401, origin);

      const order = await env.nova_max_db
        .prepare("SELECT * FROM orders WHERE id = ?")
        .bind(orderId)
        .first<OrderRow>();
      if (!order) return errorResponse("Order not found", 404, origin);
      if (order.status !== "pending") {
        return errorResponse("Order not pending", 409, origin);
      }
      if (order.driver_id && order.driver_id !== driver.id) {
        return errorResponse("Order already assigned to another driver", 409, origin);
      }

      await env.nova_max_db
        .prepare(
          "UPDATE orders SET driver_id = NULL, cancel_reason = ?, cancelled_by = ?, cancelled_at = CURRENT_TIMESTAMP WHERE id = ?"
        )
        .bind(cancelReason, "driver", orderId)
        .run();

      const ts = new Date().toISOString();
      if (order.store_id) {
        await broadcastEvent(env, order.store_id, {
          type: "order_status",
          order_id: orderId,
          store_id: order.store_id,
          status: "pending",
          driver_id: null,
          cancel_reason: cancelReason,
          cancelled_at: ts,
          cancelled_by: "driver",
          ts,
        });
      }
      await broadcastGlobalEvent(env, {
        type: "order_status",
        audience: "admin",
        order_id: orderId,
        store_id: order.store_id,
        status: "pending",
        driver_id: null,
        cancel_reason: cancelReason,
        cancelled_at: ts,
        cancelled_by: "driver",
        ts,
      });
      await broadcastGlobalEvent(env, {
        type: "order_status",
        audience: "driver",
        order_id: orderId,
        store_id: order.store_id,
        status: "pending",
        driver_id: null,
        cancel_reason: cancelReason,
        cancelled_at: ts,
        cancelled_by: "driver",
        ts,
      });

      return jsonResponse(
        { ok: true, order_id: orderId, status: "pending" },
        200,
        origin
      );
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
        getNormalized(body?.admin_code) ??
        getNormalized(request.headers.get("x-admin-code"));
      let driverId =
        getString(body?.driver_id) ?? getString(request.headers.get("x-driver-id"));
      const driverCode =
        getNormalized(body?.driver_code) ??
        getNormalized(body?.secret_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const secretCode =
        getNormalized(body?.secret_code) ??
        getNormalized(request.headers.get("x-driver-code"));
      const cancelReason =
        getString(body?.cancel_reason) ??
        getString(body?.reason) ??
        getString(body?.note);

      const store = await requireStore(env, storeId, adminCode);
      let driver = await requireDriver(env, driverId, secretCode);
      if (!driver && driverCode) {
        driver = await requireDriverByCode(env, driverCode);
        if (driver && !driverId) driverId = driver.id;
      }

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
      const isReopen = currentStatus === "cancelled" && nextStatus === "pending";
      if (nextStatus === "cancelled" && !cancelReason) {
        return errorResponse("Missing cancel_reason", 400, origin);
      }
      if (isReopen) {
        if (!store) return errorResponse("Unauthorized", 401, origin);
        if (order.store_id && order.store_id !== store.id) {
          return errorResponse("Unauthorized", 401, origin);
        }

        await env.nova_max_db
          .prepare(
            "UPDATE orders SET status = ?, driver_id = NULL, delivered_at = NULL, cancel_reason = NULL, cancelled_by = NULL, cancelled_at = NULL WHERE id = ?"
          )
          .bind("pending", orderId)
          .run();

        const refreshed = await env.nova_max_db
          .prepare("SELECT * FROM orders WHERE id = ?")
          .bind(orderId)
          .first<OrderRow>();

        const payloadOrder = {
          id: orderId,
          store_id: store.id,
          store_name: store.name,
          store_code: store.store_code ?? null,
          driver_id: null,
          customer_name: refreshed?.customer_name ?? order.customer_name,
          customer_phone: refreshed?.customer_phone ?? order.customer_phone,
          customer_location_text:
            refreshed?.customer_location_text ?? order.customer_location_text,
          order_type: refreshed?.order_type ?? order.order_type,
          receiver_name: refreshed?.receiver_name ?? order.receiver_name,
          payout_method: refreshed?.payout_method ?? order.payout_method,
          price: refreshed?.price ?? order.price,
          delivery_fee: refreshed?.delivery_fee ?? order.delivery_fee,
          cash_amount: refreshed?.cash_amount ?? order.cash_amount,
          wallet_amount: refreshed?.wallet_amount ?? order.wallet_amount,
          status: "pending",
          created_at: refreshed?.created_at ?? order.created_at ?? new Date().toISOString(),
        };

        const ts = new Date().toISOString();
        await broadcastEvent(env, store.id, {
          type: "order_status",
          order_id: orderId,
          store_id: store.id,
          status: "pending",
          driver_id: null,
          delivered_at: null,
          cancel_reason: null,
          cancelled_at: null,
          cancelled_by: null,
          ts,
        });

        await broadcastGlobalEvent(env, {
          type: "order_created",
          audience: "admin",
          ts,
          order: payloadOrder,
        });

        await broadcastGlobalEvent(env, {
          type: "order_created",
          audience: "driver",
          ts,
          order: payloadOrder,
        });

        return jsonResponse(
          { ok: true, order_id: orderId, status: "pending" },
          200,
          origin
        );
      }
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

      if (currentStatus === "pending" && nextStatus === "accepted") {
        const update = await env.nova_max_db
          .prepare(
            "UPDATE orders SET status = ?, driver_id = ?, cancel_reason = NULL, cancelled_by = NULL, cancelled_at = NULL WHERE id = ? AND status = 'pending' AND (driver_id IS NULL OR driver_id = ?)"
          )
          .bind(nextStatus, effectiveDriverId, orderId, effectiveDriverId)
          .run();

        if (update.meta.changes === 0) {
          return errorResponse("Order already assigned to another driver", 409, origin);
        }
      } else if (nextStatus === "cancelled") {
        const cancelledBy = driver ? "driver" : "admin";
        const cancelledDriverId = effectiveDriverId ?? order.driver_id;
        await env.nova_max_db
          .prepare(
            "UPDATE orders SET status = ?, driver_id = ?, cancel_reason = ?, cancelled_by = ?, cancelled_at = CURRENT_TIMESTAMP WHERE id = ?"
          )
          .bind(nextStatus, cancelledDriverId, cancelReason, cancelledBy, orderId)
          .run();
      } else if (markDelivered) {
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
        const orderAmount = typeof order.price === "number" ? order.price : 0;
        const payoutMethod = "cash";

        if (orderAmount > 0) {
          const idemKey = `order:${orderId}`;
          const result = await applyWalletTransaction({
            env,
            driverId: effectiveDriverId,
            storeId: order.store_id,
            amount: orderAmount,
            type: "credit",
            method: payoutMethod,
            note: buildOrderNote(orderId, "delivered", store?.name ?? null),
            idempotencyKey: idemKey,
            relatedOrderId: orderId,
          });

          if (!result.ok && !result.duplicate) {
            return errorResponse(result.error, 400, origin);
          }
        }
      }

      const ts = new Date().toISOString();
      const cancelledAt = nextStatus === "cancelled" ? ts : order.cancelled_at;
      const cancelReasonOut =
        nextStatus === "cancelled" ? cancelReason : order.cancel_reason;
      const cancelledByOut =
        nextStatus === "cancelled"
          ? driver
            ? "driver"
            : "admin"
          : order.cancelled_by;
      if (order.store_id) {
        await broadcastEvent(env, order.store_id, {
          type: "order_status",
          order_id: orderId,
          store_id: order.store_id,
          status: nextStatus,
          driver_id: effectiveDriverId,
          target_driver_id: effectiveDriverId ?? undefined,
          delivered_at: markDelivered ? ts : order.delivered_at,
          cancelled_at: cancelledAt ?? undefined,
          cancel_reason: cancelReasonOut ?? undefined,
          cancelled_by: cancelledByOut ?? undefined,
          ts,
        });
      }

      await broadcastGlobalEvent(env, {
        type: "order_status",
        audience: "admin",
        order_id: orderId,
        store_id: order.store_id,
        status: nextStatus,
        driver_id: effectiveDriverId,
        delivered_at: markDelivered ? ts : order.delivered_at,
        cancelled_at: cancelledAt ?? undefined,
        cancel_reason: cancelReasonOut ?? undefined,
        cancelled_by: cancelledByOut ?? undefined,
        ts,
      });

      await broadcastGlobalEvent(env, {
        type: "order_status",
        audience: "driver",
        order_id: orderId,
        store_id: order.store_id,
        status: nextStatus,
        driver_id: effectiveDriverId,
        delivered_at: markDelivered ? ts : order.delivered_at,
        cancelled_at: cancelledAt ?? undefined,
        cancel_reason: cancelReasonOut ?? undefined,
        cancelled_by: cancelledByOut ?? undefined,
        ts,
      });

      return jsonResponse(
        { ok: true, order_id: orderId, status: nextStatus },
        200,
        origin
      );
    }

    return errorResponse("Not Found", 404, origin);
  },
} satisfies ExportedHandler<Env>;

export default Sentry.withSentry((env: Env) => ({
  dsn: env.SENTRY_DSN,
  environment: env.SENTRY_ENVIRONMENT,
  tracesSampleRate: 0.1,
}), handler);

