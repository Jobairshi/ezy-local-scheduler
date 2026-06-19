# ezy-local-scheduler

A tiny **local stand-in for AWS EventBridge Scheduler + SQS**. You register a
payload with a fire time and a target HTTP endpoint; at that exact time the
service `POST`s the payload to the target — with retries and a dead-letter list
on repeated failure. That's the same shape as production
(*EventBridge fires → SQS → Lambda → your internal endpoint*), so you can test
the full **schedule → fire → deliver** pipeline of **any backend** locally.

- **No AWS account.** No LocalStack.
- **No Docker required** (Docker is provided but optional).
- **Zero npm dependencies** — Node ≥ 20.6 built-ins only.
- **Generic** — each schedule carries its own `url` / `headers` / `body`, so one
  instance serves any number of backends. A default target can be configured so
  a single backend only needs to send `{name, fireAt, body}`.

> It does **not** speak the AWS wire protocol — it behaves *like* AWS. To use
> it, you make a small **temporary** change in your backend so its scheduler
> calls this service instead of AWS (see [Wiring a backend](#wiring-a-backend)).
> Nothing from this tool needs to live in your project.

---

## Run it

### Plain Node (simplest)

```bash
cp .env.example .env        # optional — edit defaults if you like
npm start                   # or: npm run dev   (auto-restart on change)
```

You'll see:

```
ezy-local-scheduler listening on :4500
```

### Docker

```bash
# Optionally export DEFAULT_TARGET_URL / DEFAULT_AUTH_HEADER first.
docker compose up --build
```

The compose file points the default target at `host.docker.internal:3333`
(reaches your host from the container — works whether your backend runs on the
host or in another container) and persists the store on a named volume.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/schedules` | Create / overwrite a schedule (overwrite by `name`). |
| `DELETE` | `/schedules/:name` | Cancel a schedule (idempotent). |
| `GET` | `/schedules` | List active schedules (with countdown) + dead-letter. |
| `GET` | `/dead-letter` | List failed deliveries. |
| `DELETE` | `/dead-letter` | Clear the dead-letter list. |
| `GET` | `/health` | `{ok, schedules, deadLetter}`. |

### `POST /schedules` body

```jsonc
{
  "name": "event-reminder-42",          // dedupe/overwrite key (optional; auto-generated if omitted)
  "fireAt": "2026-06-30T09:00:00",       // ISO time (naive = UTC). OR use "delaySeconds": 120
  "url": "http://localhost:3333/api/internal/feed/publish-scheduled", // optional if DEFAULT_TARGET_URL set
  "method": "POST",                      // optional, default POST
  "headers": {"authorization": "Bearer secret"}, // optional; falls back to DEFAULT_AUTH_HEADER
  "body": {"kind": "event_reminder", "id": 42}    // delivered verbatim as JSON
}
```

Re-posting with the same `name` overwrites (resets retries) — exactly like
EventBridge's deterministic-name behavior. After a successful delivery the
schedule is deleted (fire-once). On non-2xx / network error it retries with
exponential backoff up to `MAX_ATTEMPTS`, then moves to the dead-letter list.

### Quick manual test

```bash
# Start a throwaway target in one terminal:
node -e "require('http').createServer((q,s)=>{let b='';q.on('data',c=>b+=c);q.on('end',()=>{console.log('GOT',b);s.end('ok')})}).listen(9999)"

# Schedule a delivery 5 seconds out:
curl -s localhost:4500/schedules -H content-type:application/json -d '{
  "name":"demo","delaySeconds":5,"url":"http://localhost:9999","body":{"hello":"world"}
}'

# ~5s later the target terminal prints: GOT {"hello":"world"}
curl -s localhost:4500/schedules    # watch it disappear after firing
```

---

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `4500` | API port. |
| `DEFAULT_TARGET_URL` | — | Delivery target when a schedule omits `url`. |
| `DEFAULT_AUTH_HEADER` | — | `Authorization` sent when a schedule omits one (e.g. `Bearer xyz`). |
| `POLL_INTERVAL_MS` | `1000` | How often due schedules are checked. |
| `MAX_ATTEMPTS` | `5` | Delivery attempts before dead-lettering. |
| `RETRY_BACKOFF_MS` | `5000` | Base backoff (exponential: `base · 2^(n-1)`). |
| `DELIVER_TIMEOUT_MS` | `15000` | Per-delivery HTTP timeout. |
| `STORE_FILE` | `./data/store.json` | Durable store (atomic writes; survives restarts). |

---

## Wiring a backend

Make a **temporary** change so your scheduler posts here instead of AWS, then
revert it when done. Two patterns:

### Generic (any backend, any language)

Wherever you'd call AWS to schedule something, instead:

```js
await fetch(`${LOCAL_SCHEDULER_URL}/schedules`, {
  method: 'POST',
  headers: {'content-type': 'application/json'},
  body: JSON.stringify({
    name: `myjob-${id}`,                 // deterministic → overwrite on reschedule
    fireAt: fireAtUtcIso,                // when to fire
    url: `${MY_API}/internal/callback`,  // where to deliver
    headers: {authorization: `Bearer ${SECRET}`},
    body: {kind, id /* …payload */},     // what your consumer expects
  }),
});
// and to cancel:
await fetch(`${LOCAL_SCHEDULER_URL}/schedules/myjob-${id}`, {method: 'DELETE'});
```

### EzyCommunity `community-api` (temporary edit to `services/aws/scheduler.ts`)

The repo's `scheduleAt` / `cancelSchedule` already centralize scheduling. Drop a
short branch at the top of each — and start this tool with the matching default
target + secret so the body needs no `url`:

```bash
# this tool's .env
DEFAULT_TARGET_URL=http://localhost:3333/api/internal/feed/publish-scheduled
DEFAULT_AUTH_HEADER=Bearer <your INTERNAL_API_SECRET>
```

```ts
// top of scheduleAt(), after the fireAt guard — TEMPORARY:
if (process.env.LOCAL_SCHEDULER_URL) {
  await fetch(`${process.env.LOCAL_SCHEDULER_URL}/schedules`, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({
      name: `${SCHEDULE_NAME_PREFIX[input.kind]}-${input.id}`,
      fireAt: input.fireAt,
      body: {kind: input.kind, id: input.id, ...(input.payload ?? {})},
    }),
  });
  return;
}

// top of cancelSchedule() — TEMPORARY:
if (process.env.LOCAL_SCHEDULER_URL) {
  await fetch(
    `${process.env.LOCAL_SCHEDULER_URL}/schedules/${SCHEDULE_NAME_PREFIX[input.kind]}-${input.id}`,
    {method: 'DELETE'}
  );
  return;
}
```

Then run the backend with `LOCAL_SCHEDULER_URL=http://localhost:4500`, create an
event with a reminder a couple minutes out, and watch this tool deliver it to
the internal endpoint — driving the real `fireDueEventReminders` consumer, exactly
as AWS would in prod. Remove the branches when you're done testing.

---

## How it maps to AWS

| Production | This tool |
|---|---|
| EventBridge `CreateSchedule` (`at(...)`, one-time, auto-delete) | `POST /schedules` (upsert) + fire-once |
| EventBridge `DeleteSchedule` | `DELETE /schedules/:name` |
| SQS queue + Lambda poll | 1-second poll loop |
| Lambda → POST internal endpoint | poller → POST target (identical request) |
| SQS visibility-timeout retries → DLQ | exponential backoff → dead-letter list |

What it intentionally does **not** cover: the literal AWS SDK serialization +
IAM. That's config you validate in staging, not application logic.
