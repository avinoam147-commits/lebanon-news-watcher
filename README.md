# Lebanon Telegram watcher

Lightweight poller for public `https://t.me/s/<channel>` pages. It uses no AI.
When it sees a major breaking item, it POSTs a batched webhook to wake a
Grok Bot routine. The Friday *shabbat_entry* timer uses the same webhook
with a different payload.

This public repository holds only the script, the channel/keyword config,
and the GitHub Actions workflow. Webhook credentials are Actions secrets
(`LEB_WEBHOOK_URL`, `LEB_WEBHOOK_KEY`) and are never committed. Runtime
state (seen post ids, rate-limit counters, shabbat_entry dates) is stored
in the Actions cache, not in git, and does not contain secrets.

Primary home: [avinoam147-commits/lebanon-news-watcher](https://github.com/avinoam147-commits/lebanon-news-watcher).

## How it runs (GitHub Actions)

The `Watcher` workflow is the primary runner, so polling continues even
when a local box is asleep.

- Schedule: `*/5 * * * *` (UTC) plus `workflow_dispatch`.
- Each job loops for 5 hours 50 minutes and polls every **2 minutes**
  during the active window.
- A concurrency group (`lebanon-news-watcher`, `cancel-in-progress: false`)
  allows only one loop at a time. The next cron run waits in the queue
  (GitHub keeps the latest pending run) and starts when the current job
  exits, so there is no gap.
- State is restored at the start of a job (`actions/cache/restore` with a
  unique key plus a `watcher-state-` prefix) and saved at the end
  (`actions/cache/save`, `if: always()`), with ~10 minutes left before the
  6-hour job limit.

Secrets must be set on the repository (never printed, never committed):

```bash
printf '%s' "$LEB_WEBHOOK_URL" | gh secret set LEB_WEBHOOK_URL --repo avinoam147-commits/lebanon-news-watcher
printf '%s' "$LEB_WEBHOOK_KEY" | gh secret set LEB_WEBHOOK_KEY --repo avinoam147-commits/lebanon-news-watcher
```

Manual start of the long loop:

```bash
gh workflow run Watcher --repo avinoam147-commits/lebanon-news-watcher
```

A separate `Watcher once` workflow runs a single dry-run cycle (no webhook
POST) and is useful to confirm all channels fetch. It is not in the long
loop's concurrency group.

Pause (for example before starting a local fallback):

```bash
gh workflow disable Watcher --repo avinoam147-commits/lebanon-news-watcher
```

Resume:

```bash
gh workflow enable Watcher --repo avinoam147-commits/lebanon-news-watcher
gh workflow run Watcher --repo avinoam147-commits/lebanon-news-watcher
```

## Hours and Shabbat

- Fetches every 2 minutes, 07:00–23:45 Asia/Jerusalem.
- Does not call the *breaking-news* webhook from Tel Aviv candle-lighting
  on Friday until havdalah on Saturday (times from hebcal.com, geonameid
  293397, cached in `shabbat-cache.json`). Seen ids are still updated
  during Shabbat so nothing dumps after havdalah.

## Shabbat-entry timer

Independent of the 2-minute poll cycle and of the 07:00–23:45 fetch
window, the watcher checks the clock at least every minute. On Friday,
once per date, when Asia/Jerusalem time reaches *Jerusalem* candle
lighting minus 50 minutes (`shabbat_entry.lead_minutes` in
`config.json`), it POSTs once to the same webhook. Jerusalem times come
from hebcal.com, geonameid 281184, with the 40-minute-before-sunset
default (`b=40`).

```
{"event":"shabbat_entry","date":"YYYY-MM-DD","candle_lighting_jerusalem":"HH:MM","candle_lighting_tel_aviv":"HH:MM"}
```

This event is not subject to the breaking-news rate limit (1 POST / 10
minutes, 20/day) and is not counted against it. A successful send is
recorded in state (`shabbat_entry.last_fired_date`) so a later job will
not re-send for that date. If the watcher starts after the fire time but
still before the cutoff (Jerusalem candle lighting minus 30 minutes), it
fires immediately. It never fires at or after that cutoff.

A Grok Bot routine also backs this timer and ignores a duplicate POST
via a `.done` flag.

## Auth and payload

Grok Bot routine webhooks expect:

```
Authorization: Bearer <sender key>
Content-Type: application/json
```

The watcher also sends `X-Webhook-Key: <key>`. Header name and value
template are configurable in `config.json`.

Breaking-news body: `{"candidates":[{"channel","time","text","url"}]}`
with text trimmed to 600 characters. At most one POST per 10 minutes,
and at most 20 POSTs per day; all pending candidates are included in the
batch.

## Local fallback

A copy of the script also lives on the operator box at
`/workspace/lebanon-news/watcher/`. GitHub Actions is primary; keep the
local process **stopped** while Actions is running, or both will POST.

To use the box as a fallback (Actions down, or the workflow disabled):

1. Disable the workflow so you do not get duplicate pushes:
   `gh workflow disable Watcher --repo avinoam147-commits/lebanon-news-watcher`
2. If the box `state.json` is stale (Actions has been running for a
   while), move it aside so the first local cycle marks currently visible
   posts as seen and does not dump old items:
   `mv /workspace/lebanon-news/watcher/state.json /workspace/lebanon-news/watcher/state.json.bak`
3. Start:
   `/workspace/lebanon-news/watcher/start.sh`
4. Stop later with:
   `/workspace/lebanon-news/watcher/stop.sh`

Webhook credentials on the box come from the process environment or an
optional `/workspace/lebanon-news/watcher/.env` (never commit that file).
One-shot cycle:

```bash
python3 /workspace/lebanon-news/watcher/watcher.py --once
```

Logs: `/workspace/lebanon-news/watcher/watcher.log`.
