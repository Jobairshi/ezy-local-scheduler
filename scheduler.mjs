/**
 * ezy-local-scheduler — a local stand-in for AWS EventBridge Scheduler + SQS.
 *
 * Register a payload with a fire time and a target HTTP endpoint; at that exact
 * time the service POSTs the payload to the target, with retries + a
 * dead-letter list on repeated failure. This mirrors what AWS does in prod
 * (EventBridge Scheduler fires → SQS → Lambda → your internal endpoint), so you
 * can exercise the full schedule→fire→deliver pipeline of ANY backend locally,
 * with no AWS account and no Docker required.
 *
 * Generic by design: every schedule carries its own `url`/`headers`/`body`, so
 * one running instance serves any number of backends. A default target can also
 * be configured so a single backend can post just `{name, fireAt, body}`.
 *
 * Zero npm dependencies — Node >= 20.6 built-ins only (http, fetch, fs).
 */

import {createServer} from 'node:http';
import {existsSync, mkdirSync, readFileSync, renameSync, writeFileSync} from 'node:fs';
import {dirname} from 'node:path';

// ── Minimal .env loader (so `node scheduler.mjs` works without --env-file) ───
function loadDotEnv(file = '.env') {
  if (!existsSync(file)) return;
  for (const raw of readFileSync(file, 'utf8').split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    let val = line.slice(eq + 1).trim();
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = val;
  }
}
loadDotEnv();

// ── Config ───────────────────────────────────────────────────────────────────
const CONFIG = {
  port: Number(process.env.PORT || 4500),
  // Used when a schedule omits `url` — lets a single backend post just the body.
  defaultTargetUrl: process.env.DEFAULT_TARGET_URL || '',
  // Used when a schedule omits an Authorization header (e.g. "Bearer <secret>").
  defaultAuthHeader: process.env.DEFAULT_AUTH_HEADER || '',
  pollIntervalMs: Number(process.env.POLL_INTERVAL_MS || 1000),
  maxAttempts: Number(process.env.MAX_ATTEMPTS || 5),
  retryBackoffMs: Number(process.env.RETRY_BACKOFF_MS || 5000),
  deliverTimeoutMs: Number(process.env.DELIVER_TIMEOUT_MS || 15000),
  storeFile: process.env.STORE_FILE || './data/store.json',
};

const log = (...args) => console.log(new Date().toISOString(), ...args);

// ── Durable store (atomic JSON file) ─────────────────────────────────────────
// Shape: { schedules: { [name]: Schedule }, deadLetter: DeadItem[] }
// Schedule: { name, deliverAtMs, url|null, method, headers, body, attempts,
//             nextAttemptAtMs, createdAtMs }
let store = {schedules: {}, deadLetter: []};

function loadStore() {
  try {
    if (existsSync(CONFIG.storeFile)) {
      const parsed = JSON.parse(readFileSync(CONFIG.storeFile, 'utf8'));
      store = {schedules: parsed.schedules ?? {}, deadLetter: parsed.deadLetter ?? []};
      log(
        `Loaded store: ${Object.keys(store.schedules).length} schedule(s), ` +
          `${store.deadLetter.length} dead-letter`
      );
    }
  } catch (err) {
    log('WARN failed to load store, starting fresh:', err.message);
  }
}

function persist() {
  try {
    const dir = dirname(CONFIG.storeFile);
    if (dir && dir !== '.' && !existsSync(dir)) mkdirSync(dir, {recursive: true});
    const tmp = `${CONFIG.storeFile}.tmp`;
    writeFileSync(tmp, JSON.stringify(store), 'utf8');
    renameSync(tmp, CONFIG.storeFile); // atomic swap
  } catch (err) {
    log('ERROR failed to persist store:', err.message);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function parseFireAt(input) {
  // Accept either `fireAt` (ISO) or `delaySeconds`. Naive (no zone) is treated
  // as UTC, matching how EventBridge `at()` expressions are written.
  if (typeof input.delaySeconds === 'number') {
    return Date.now() + input.delaySeconds * 1000;
  }
  if (typeof input.fireAt === 'string' && input.fireAt) {
    const hasZone = /[zZ]$|[+-]\d\d:?\d\d$/.test(input.fireAt);
    const ms = Date.parse(hasZone ? input.fireAt : `${input.fireAt}Z`);
    if (!Number.isNaN(ms)) return ms;
  }
  return NaN;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => {
      data += chunk;
      if (data.length > 2_000_000) {
        reject(new Error('payload too large'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function json(res, status, payload) {
  const text = JSON.stringify(payload);
  res.writeHead(status, {'content-type': 'application/json'});
  res.end(text);
}

// ── Delivery + poll loop ──────────────────────────────────────────────────────
const inFlight = new Set();

async function deliver(schedule) {
  const url = schedule.url || CONFIG.defaultTargetUrl;
  if (!url) throw new Error('no target url (schedule.url and DEFAULT_TARGET_URL both empty)');

  const headers = {'content-type': 'application/json', ...(schedule.headers ?? {})};
  if (!headers.authorization && !headers.Authorization && CONFIG.defaultAuthHeader) {
    headers.authorization = CONFIG.defaultAuthHeader;
  }

  const res = await fetch(url, {
    method: schedule.method || 'POST',
    headers,
    body: JSON.stringify(schedule.body ?? {}),
    signal: AbortSignal.timeout(CONFIG.deliverTimeoutMs),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(`target responded ${res.status} ${detail.slice(0, 300)}`);
  }
}

function processSchedule(schedule) {
  const {name} = schedule;
  inFlight.add(name);
  deliver(schedule)
    .then(() => {
      delete store.schedules[name]; // fire-once (mirrors ActionAfterCompletion: DELETE)
      persist();
      log(`✓ delivered "${name}" → ${schedule.url || CONFIG.defaultTargetUrl}`);
    })
    .catch(err => {
      schedule.attempts = (schedule.attempts ?? 0) + 1;
      if (schedule.attempts >= CONFIG.maxAttempts) {
        delete store.schedules[name];
        store.deadLetter.push({
          name,
          url: schedule.url || CONFIG.defaultTargetUrl,
          body: schedule.body,
          attempts: schedule.attempts,
          lastError: err.message,
          failedAtMs: Date.now(),
        });
        log(`✗ DEAD-LETTER "${name}" after ${schedule.attempts} attempts: ${err.message}`);
      } else {
        // Exponential backoff, mimicking SQS visibility-timeout retries.
        schedule.nextAttemptAtMs =
          Date.now() + CONFIG.retryBackoffMs * 2 ** (schedule.attempts - 1);
        log(
          `… retry "${name}" (attempt ${schedule.attempts}/${CONFIG.maxAttempts}) ` +
            `after ${err.message}`
        );
      }
      persist();
    })
    .finally(() => inFlight.delete(name));
}

function tick() {
  const now = Date.now();
  for (const schedule of Object.values(store.schedules)) {
    if (inFlight.has(schedule.name)) continue;
    if (schedule.deliverAtMs > now) continue;
    if (schedule.nextAttemptAtMs && schedule.nextAttemptAtMs > now) continue;
    processSchedule(schedule);
  }
}

// ── HTTP API ───────────────────────────────────────────────────────────────────
function listView() {
  const now = Date.now();
  return Object.values(store.schedules)
    .sort((a, b) => a.deliverAtMs - b.deliverAtMs)
    .map(s => ({
      name: s.name,
      deliverAt: new Date(s.deliverAtMs).toISOString(),
      inSeconds: Math.round((s.deliverAtMs - now) / 1000),
      url: s.url || CONFIG.defaultTargetUrl || null,
      attempts: s.attempts ?? 0,
      body: s.body,
    }));
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://localhost:${CONFIG.port}`);
    const {pathname} = url;
    const method = req.method ?? 'GET';

    if (method === 'GET' && pathname === '/health') {
      return json(res, 200, {
        ok: true,
        schedules: Object.keys(store.schedules).length,
        deadLetter: store.deadLetter.length,
      });
    }

    if (method === 'GET' && (pathname === '/' || pathname === '/schedules')) {
      return json(res, 200, {schedules: listView(), deadLetter: store.deadLetter});
    }

    if (method === 'GET' && pathname === '/dead-letter') {
      return json(res, 200, {deadLetter: store.deadLetter});
    }

    if (method === 'DELETE' && pathname === '/dead-letter') {
      store.deadLetter = [];
      persist();
      return json(res, 200, {ok: true});
    }

    // Create / overwrite a schedule (overwrite-by-name === EventBridge semantics).
    if (method === 'POST' && pathname === '/schedules') {
      const parsed = JSON.parse((await readBody(req)) || '{}');
      const name = parsed.name || `sched-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
      const deliverAtMs = parseFireAt(parsed);
      if (Number.isNaN(deliverAtMs)) {
        return json(res, 400, {error: 'provide a valid `fireAt` (ISO) or `delaySeconds`'});
      }
      if (!parsed.url && !CONFIG.defaultTargetUrl) {
        return json(res, 400, {
          error: 'no target: pass `url` or configure DEFAULT_TARGET_URL',
        });
      }
      store.schedules[name] = {
        name,
        deliverAtMs,
        url: parsed.url || null,
        method: parsed.method || 'POST',
        headers: parsed.headers || null,
        body: parsed.body ?? {},
        attempts: 0,
        nextAttemptAtMs: 0,
        createdAtMs: Date.now(),
      };
      persist();
      log(`+ scheduled "${name}" for ${new Date(deliverAtMs).toISOString()}`);
      return json(res, 200, {name, deliverAt: new Date(deliverAtMs).toISOString()});
    }

    // Delete a schedule by name (idempotent — like EventBridge DeleteSchedule).
    if (method === 'DELETE' && pathname.startsWith('/schedules/')) {
      const name = decodeURIComponent(pathname.slice('/schedules/'.length));
      const existed = Boolean(store.schedules[name]);
      delete store.schedules[name];
      if (existed) {
        persist();
        log(`- cancelled "${name}"`);
      }
      return json(res, 200, {ok: true, existed});
    }

    return json(res, 404, {error: 'not found'});
  } catch (err) {
    return json(res, 400, {error: err.message});
  }
});

// ── Boot ─────────────────────────────────────────────────────────────────────
loadStore();
const timer = setInterval(tick, CONFIG.pollIntervalMs);
server.listen(CONFIG.port, () => {
  log(`ezy-local-scheduler listening on :${CONFIG.port}`);
  log(`  poll=${CONFIG.pollIntervalMs}ms maxAttempts=${CONFIG.maxAttempts} store=${CONFIG.storeFile}`);
  if (CONFIG.defaultTargetUrl) log(`  default target: ${CONFIG.defaultTargetUrl}`);
  else log('  no DEFAULT_TARGET_URL — each schedule must carry its own `url`');
});

function shutdown(signal) {
  log(`${signal} — persisting + shutting down`);
  clearInterval(timer);
  persist();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
