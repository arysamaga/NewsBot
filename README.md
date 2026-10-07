# Newsstream

Agentic financial news monitor for event-driven crude oil trading. Watches a
curated list of OSINT accounts (Telegram + X), classifies each post's
materiality with a rubric-scored LLM call, clusters posts into events, and
surfaces new events / material updates in a local web UI within seconds.
Spec: [CLAUDE.md](CLAUDE.md).

Written in [Jac](https://jaclang.org) (ported from Python 2026-10-07; see
[Jac notes](#jac-notes)).

## Quick start

```bash
jac install                  # Python and npm packages into .jac/
cp .env.example .env         # fill in ANTHROPIC_API_KEY (models are pre-set)
jac run                      # http://localhost:8000
```

With no platform credentials the adapters idle (footer shows why) — exercise
the whole pipeline and UI with the fixture replay:

```bash
jac run                                # terminal 1
jac run replay -- tests/fixtures/      # terminal 2, 10x speed
```

## Data sources (decided 2026-09-10; CLAUDE.md §14)

**Defaults: Telegram + TwitterAPI.io push. Fallback: official X API,
budget-capped.** Running the fallback alongside push is safe — posts
deduplicate by id, so double delivery is skipped, not double-surfaced.

1. **Telegram** — free, push. Get `TELEGRAM_API_ID`/`HASH` at
   <https://my.telegram.org/apps>, then list the channels that mirror your
   seed accounts in `config.yaml` → `telegram_channels` (you must supply
   these; they are not guessed). The first `jac run` prompts for
   your phone + login code in the terminal (telethon); the session is saved
   to `newsstream.session` afterwards. A Bot API fallback
   (`TELEGRAM_BOT_TOKEN`) exists but only sees channels the bot is an admin
   of.
2. **TwitterAPI.io push (default X source)** — set `X_PUSH_API_KEY` from the
   <https://twitterapi.io> dashboard and you're done: the adapter creates and
   activates vendor-side filter rules for every `source: x` account in
   `config.yaml`, then streams matches over WebSocket (measured ~29 s
   post→delivery latency at a 10 s check interval). Billing, **measured live
   2026-09-10**: $0.00015 per *delivered* post, and every rule check
   redelivers the newest post (billed again — the pipeline dedups these by
   id, so they cost vendor credits but no LLM calls). Effective cost is
   therefore driven by `interval_seconds`:
   `$/day ≈ 86400/interval × 0.00015` → **10 s (default) = $1.30/day, 30 s =
   $0.43/day, 60 s = $0.22/day**, plus real post volume (~pennies). Still
   ~100× cheaper than official polling at the same latency. Set up
   auto-recharge on the vendor dashboard — exhausted credits silently stop
   deliveries. Other observed
   behaviors: first activation delivers a ~2 h backlog burst; the free tier
   REST limit is 1 request/5 s (handled); rules are deactivated on app
   shutdown so credits don't drain while you're not running, and re-armed at
   startup. The trial credit is **$0.10** (~10,000 credits) — at the default
   30 s interval that's roughly 5 hours of live running, so top up before
   relying on it. Caveats: it's an unofficial scraper (can break or be
   blocked — hence the fallback), and its `from:` rules follow *handles*, so
   if a seed account renames itself, update `config.yaml` and restart (the
   UI going quiet for one account is the tell).
3. **Official X API (fallback)** — set `X_BEARER_TOKEN` and flip
   `x_api.enabled: true` when the push vendor is down. ID-based (rename-proof)
   polling with a hard daily read budget. Read the cost math below **first**.

## Official X API cost math (verified against docs.x.com pricing, 2026-09-10)

Pay-per-use is the only self-serve tier (legacy Basic/Pro are closed;
filtered stream is Enterprise-only, ~$42k/mo). **$0.005 per post read**, $0.01
per user read, capped at 3M post reads/month; re-reading the same post within
a UTC day is charged once.

Polling cost floor: each poll of each account bills at least one read.

```
reads/day ≈ accounts × 86400 / poll_interval_seconds   (+ actual new posts)
```

| accounts | interval | reads/day | $/day  | $/month |
|---------:|---------:|----------:|-------:|--------:|
| 8        | 30 s     | ~23,040   | $115   | ~$3,450 |
| 8        | 60 s (default) | ~11,520 | $58 | ~$1,730 |
| 8        | 150 s    | ~4,608    | $23    | ~$690   |
| 8        | 300 s    | ~2,304    | $12    | ~$350   |

The default `daily_read_budget: 5000` (≈$25/day) is a **hard stop**: at the
default 60 s interval with 8 accounts it is exhausted in ~10 hours, the X
adapter pauses until the next UTC day (loudly, with a UI banner), and
Telegram keeps running. This is why polling is the *fallback*: at equal check cadence the push feed
is ~100× cheaper ($0.43/day vs $58/day at ~30–60 s latency). If you do run
the fallback for extended periods, slow the interval to ≥150 s or raise the
budget.

LLM cost is small by comparison: triage (`claude-haiku-4-5`, ~800 in / 250
out tokens) is ~$0.002/post → ~$1/day at 500 posts/day; review/dedup/summary
calls (`claude-opus-5`) add roughly $1–3/day at tens of surfaced posts. The
footer shows the live estimate.

## Configuration

- `.env` — secrets + model slots (`TRIAGE_MODEL`, `REVIEW_MODEL`). Model IDs
  live only here and in `config.jac`.
- `config.yaml` — accounts + tiers, telegram channels, threshold (default
  70/100), rubric weights, poll interval, read budget, UI knobs. Tune the
  threshold by watching the "show suppressed" tab, which lists each
  suppressed post with its score.

## Tests

```bash
jac test                           # offline logic tests always run
ANTHROPIC_API_KEY=... jac test     # first run records tests/cassettes/llm.json
```

Classifier/dedup behavior tests replay recorded LLM responses from
`tests/cassettes/llm.json` (committed once recorded), so the suite is green
offline and deterministic after the first keyed run. Record in one full run —
the dedup sequences chain on earlier recorded outputs. `jac.toml` runs the
tests one at a time (`[dev] test_jobs = "0"`) so a recording run cannot have
two workers rewriting the cassette at once; for a faster replay-only run use
`JAC_TEST_JOBS=4 jac test`.

## Jac notes

Developed and tested on jac 0.37.25. jac 0.37.23 and older cannot read this
`jac.toml` (Python packages moved to `[dependencies.pypi]` in 0.37.24).

The app is three small Jac apps in one workspace (`jac.toml`):

| Command | What it runs |
|---|---|
| `jac run` | the service: `main.jac` → `app.jac` (FastAPI + uvicorn, as before) |
| `jac build web` | the browser page: `web/main.jac` → one self-contained `index.html` under `.jac/` |
| `jac run replay -- <dir>` | the fixture replay tool: `replay.jac` |

- **Same libraries underneath.** FastAPI, aiosqlite, the Anthropic SDK, httpx,
  websockets and telethon are imported from Jac directly. `jac.toml` pins them
  to the versions the Python edition's `uv.lock` had; `jac.lock` records the
  full resolved set. The HTTP endpoints, the SQLite schema and file, the
  prompts and the recorded cassette are unchanged.
- **The page has a build step now.** The service builds it at startup when it
  is missing or older than `web/` or `jac.toml` (about 10 s, logged), and
  serves a short "not built" page with HTTP 503 if that build fails. Run
  `jac build web` to see build errors directly.
- **Startup.** A start after a code change recompiles (about 30–40 s here);
  later starts take about 5 s. `main.jac` is deliberately tiny: jac re-analyses
  the entry file on every start, and starting through `app.jac` directly costs
  about 17 s each time.
- **The Anthropic SDK is loaded with `importlib`** in `pipeline/llm.jac`
  instead of a plain import. jac 0.37.25 refuses to start a program whose
  static imports reach `httpx2` (it rejects that package's `typing.overload`
  methods, error E0076). Try a plain `import anthropic;` again after a jac
  upgrade.
- **Checks.** `jac check` type-checks all three apps, `jac fmt --check` the
  formatting. `jac check` prints warnings for HTML tag names and unused state
  setters in `web/main.jac`; jac's own scaffolds produce the same ones.

## Operational behavior worth knowing

- Posts are persisted before any LLM call; a broken classifier fails **open**
  (surfaces at threshold with a badge), and 3 consecutive API errors switch
  to surfacing official/tracker posts unclassified ("unfiltered" badge) until
  calls succeed.
- Accounts are tracked by stable numeric ID; renames follow automatically,
  and an ID that stops resolving marks the account inactive with a UI banner.
- Events go stale after 24 h without updates and stop being dedup candidates.
- UPDATE notifications re-enter the tray as smaller cards (spec default; see
  CLAUDE.md §14).
