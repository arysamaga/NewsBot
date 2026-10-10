"""newsstream web UI (CLAUDE.md §10): notification tray, event feed, suppressed
list and status footer.

This is the browser half of the app, written in Jac and compiled to one
self-contained page by `jac build web`. It talks to the FastAPI service
(app.jac) over the same plain HTTP endpoints as before: /stream (SSE),
/notifications, /feed, /suppressed, /status and /refetch.
"""

import "./main.css";
import from react { useEffect }

# ---------- shapes of the JSON the service sends ----------

"""One tray card, as published on /stream and listed by /notifications."""
obj Notification {
    has notification_id: int,
        kind: str,  # NEW_EVENT | UPDATE | UNFILTERED
        category: str | None,
        one_line: str,
        handle: str,
        tier: str,
        url: str,
        posted_at: str,
        platform_latency_s: float | None,
        event_title: str,
        what_changed: str,
        badges: list[str];
}

obj FeedPost {
    has id: int,
        role: str,  # origin | update | duplicate
        url: str,
        account_handle: str,
        tier: str,
        posted_at: str,
        text: str,
        one_line: str | None;
}

obj FeedEvent {
    has id: int,
        category: str,
        title: str,
        summary: str,
        status: str,  # open | stale
        posts: list[FeedPost];
}

obj SuppressedPost {
    has id: int,
        account_handle: str,
        url: str,
        text: str,
        materiality_score: int | None,
        sub_scores: str | None,
        category: str | None,
        one_line: str | None,
        error: str | None;
}

obj AdapterState {
    has name: str,
        state: str,
        detail: str;
}

obj Percentiles {
    has p50: float | None,
        p95: float | None;
}

obj Latency {
    has platform: Percentiles,
        surface: Percentiles;
}

obj Counts {
    has posts_seen: int,
        surfaced: int;
}

obj Status {
    has adapters: list[AdapterState],
        latency: Latency,
        counts: Counts,
        llm_spend_today: float,
        llm_fail_open: bool,
        x_reads_today: int,
        refetch_running: bool,
        x_daily_read_budget: int,
        threshold: int,
        max_stacked: int,
        inactive_accounts: list[str];
}

# ---------- module state that must not trigger re-renders ----------

glob _seen: any = new(Set),  # notification ids already handled
     _acked: any = new(Set),  # notification ids the user dismissed
     _last_id: int = 0,  # highest id seen; the SSE backfill resumes from here
     _feed_timer: any = None,
     _stream: any = None;

glob REFETCH_LABEL: str = "⟳ refetch 12h";

# ---------- helpers ----------

def rel(ts_iso: str) -> str {
    s = Math.max(0, (Date.now() - new(Date, ts_iso).getTime()) / 1000);
    if s < 60 {
        return str(Math.round(s)) + "s ago";
    }
    if s < 3600 {
        return str(Math.round(s / 60)) + "m ago";
    }
    if s < 86400 {
        return str(Math.round(s / 3600)) + "h ago";
    }
    return str(Math.round(s / 86400)) + "d ago";
}

def latency_str(sec: float | None) -> str {
    if sec is None {
        return "";
    }
    if sec < 120 {
        return "+" + str(Math.round(sec)) + "s";
    }
    return "+" + str(Math.round(sec / 60)) + "m";
}

def fmt_seconds(x: float | None) -> str {
    return "–" if x is None else str(x) + "s";
}

def plural(n: int, word: str) -> str {
    return str(n) + " " + word + ("s" if n > 1 else "");
}

"""GET a JSON endpoint; None when the service cannot be reached."""
async def get_json(path: str) -> any {
    try {
        resp = await fetch(path);
        return await resp.json();
    } except Exception {
        return None;
    }
}

"""Single short beep; silently does nothing until the page has had a user gesture."""
def chime {
    try {
        # the checker has no browser typings for AudioContext
        ctx: any = new(AudioContext);  # jac:ignore[E1053]
        osc = ctx.createOscillator();
        gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.value = 880;
        gain.gain.setValueAtTime(0.08, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.5);
        osc.start();
        osc.stop(ctx.currentTime + 0.5);
    } except Exception {}
}

# ---------- pieces ----------

def TierChip(tier: str) -> JsxElement {
    <span className={"tier " + tier}>{tier}</span>
}

"""A tray card. Clicking anywhere on it acknowledges it."""
def TrayCard(n: Notification, onAck: Callable[[int], None]) -> JsxElement {
    kind_cls = "card";
    if n.kind == "UPDATE" {
        kind_cls = "card update";
    } elif n.kind == "UNFILTERED" {
        kind_cls = "card unfiltered";
    }

    <div className={kind_cls} onClick={lambda { onAck(n.notification_id); }}>
        <div className="top">
            {if n.category {
                <span className="badge">{n.category}</span>
            }}
            {if n.kind == "UPDATE" {
                <span className="badge kind-update">update</span>
            }}
            {if n.kind == "UNFILTERED" {
                <span className="badge warn">unfiltered</span>
            }}
            {for b in (n.badges or []) {
                <span key={b} className="badge warn">{b}</span>
            }}
            <TierChip tier={n.tier}/>
        </div>
        {if n.kind == "UPDATE" {
            <div>{n.event_title}</div>
            <div className="whatchanged">{"↳ " + (n.what_changed or n.one_line)}</div>
        } else {
            <div className="oneline">{n.one_line}</div>
        } }
        <div className="meta">
            <span>{"@" + n.handle}</span>
            <span>{rel(n.posted_at)}</span>
            <span title="platform latency">{latency_str(n.platform_latency_s)}</span>
            <a
                href={n.url}
                target="_blank"
                rel="noopener"
                onClick={lambda (e: MouseEvent) { e.stopPropagation(); }}
            >
                {"open ↗"}
            </a>
        </div>
    </div>
}

def PostRow(p: FeedPost) -> JsxElement {
    <div className="post">
        <span className={"role " + p.role}>{p.role}</span>{" "}
        <a href={p.url} target="_blank" rel="noopener">{"@" + p.account_handle}</a>{" "}
        <TierChip tier={p.tier}/>{" "}
        <span className="dim">{rel(p.posted_at)}</span>
        <div>{p.one_line or p.text[:220]}</div>
    </div>
}

"""One event with its origin post; updates and duplicates fold away."""
def EventBlock(ev: FeedEvent) -> JsxElement {
    origin: list[FeedPost] = [];
    extras: list[FeedPost] = [];
    n_upd = 0;
    for p in ev.posts {
        if p.role == "origin" {
            origin = origin + [p];
        } else {
            extras = extras + [p];
            if p.role == "update" {
                n_upd = n_upd + 1;
            }
        }
    }
    n_dup = len(extras) - n_upd;
    label = plural(n_upd, "update") if n_upd else "";
    if n_dup {
        label = label + (", " if label else "") + plural(n_dup, "duplicate");
    }
    event_cls = "event stale" if ev.status == "stale" else "event";

    <div className={event_cls}>
        <h3>
            <span className="badge">{ev.category}</span>
            {" " + ev.title}
        </h3>
        <div className="summary">{ev.summary}</div>
        {for p in origin {
            <PostRow key={p.id} p={p}/>
        }}
        {if len(extras) > 0 {
            <details className="extras">
                <summary>{label}</summary>
                {for p in extras {
                    <PostRow key={p.id} p={p}/>
                }}
            </details>
        }}
    </div>
}

def SuppressedRow(p: SuppressedPost) -> JsxElement {
    score = "?" if p.materiality_score is None else str(p.materiality_score);

    <div className="sup" title={p.sub_scores or ""}>
        <span className="score">{score}</span>{" "}
        <span className="badge">{p.category or "-"}</span>{" "}
        <a href={p.url} target="_blank" rel="noopener">{"@" + p.account_handle}</a>
        {" " + (p.one_line or p.text[:180]) + " "}
        {if p.error {
            <span className="badge warn">{p.error}</span>
        }}
    </div>
}

def Footer(s: Status | None, unreachable: bool) -> JsxElement {
    if unreachable {
        return <footer>server unreachable</footer>;
    }
    if s is None {
        return <footer>connecting…</footer>;
    }
    lat = s.latency;
    platform = "platform p50 "
        + fmt_seconds(lat.platform.p50)
        + " / p95 "
        + fmt_seconds(lat.platform.p95);
    pipeline = "pipeline p50 "
        + fmt_seconds(lat.surface.p50)
        + " / p95 "
        + fmt_seconds(lat.surface.p95);
    counts = "seen "
        + str(s.counts.posts_seen)
        + " · surfaced "
        + str(s.counts.surfaced)
        + " today";
    reads = "X reads " + str(s.x_reads_today) + "/" + str(s.x_daily_read_budget);

    <footer>
        {for a in s.adapters {
            <span key={a.name} title={a.detail}>
                <span className={"dot " + a.state}></span>
                {a.name}
            </span>
        }}
        <span>{platform}</span>
        <span>{pipeline}</span>
        <span>{counts}</span>
        <span>{"LLM ~$" + str(s.llm_spend_today)}</span>
        <span>{reads}</span>
        <span>{"threshold " + str(s.threshold)}</span>
    </footer>
}

"""Text for the red banner at the top, or "" when nothing is wrong."""
def banner_text(s: Status | None) -> str {
    if s is None {
        return "";
    }
    if s.llm_fail_open {
        return "Classifier unavailable — official/tracker posts surfacing UNFILTERED";
    }
    if s.x_reads_today >= s.x_daily_read_budget and s.x_daily_read_budget > 0 {
        return "X daily read budget exhausted — X polling paused until next UTC day (Telegram still live)";
    }
    if len(s.inactive_accounts) > 0 {
        return "Account(s) no longer resolvable: " + ", ".join(s.inactive_accounts);
    }
    return "";
}

# ---------- the page ----------

def:pub app -> JsxElement {
    has tray: list[Notification] = [],  # newest first
        expanded: bool = False,
        events: list[FeedEvent] = [],
        feedLoaded: bool = False,
        suppressed: list[SuppressedPost] = [],
        showSuppressed: bool = False,
        sound: bool = False,  # chime is off by default
        status: Status | None = None,
        unreachable: bool = False,
        refetch: str = "idle",  # idle | running | failed
        clock: int = 0;  # bumped every 30 s so relative times refresh

    max_stacked = (status.max_stacked or 5) if status else 5;
    visible = tray if expanded else tray[:max_stacked];
    hidden = len(tray) - len(visible);
    banner = banner_text(status);
    refetch_label = REFETCH_LABEL;
    if refetch == "running" {
        refetch_label = "refetching…";
    } elif refetch == "failed" {
        refetch_label = "refetch failed";
    }

    async def load_feed {
        data = await get_json("/feed");
        if data {
            events = data.events as list[FeedEvent];
            feedLoaded = True;
        }
    }

    async def load_status {
        data = await get_json("/status");
        if not data {
            unreachable = True;
            return;
        }
        s = data as Status;
        unreachable = False;
        status = s;
        if s.refetch_running {
            refetch = "running";
        } elif refetch == "running" {
            refetch = "idle";
        }
    }

    def ack(nid: int) {
        _acked.add(nid);
        remaining: list[Notification] = [];
        for c in tray {
            if c.notification_id != nid {
                remaining = remaining + [c];
            }
        }
        tray = remaining;
        if len(remaining) <= max_stacked {
            expanded = False;
        }
    }

    def handle(n: Notification, fresh: bool) {
        if _seen.has(n.notification_id) {
            return;
        }
        _seen.add(n.notification_id);
        if n.notification_id > _last_id {
            _last_id = n.notification_id;
        }
        if fresh and not _acked.has(n.notification_id) {
            tray = [n] + tray;
            if n.kind == "NEW_EVENT" and sound {
                chime();
            }
        }
        # one feed reload shortly after a burst of notifications
        if _feed_timer {
            clearTimeout(_feed_timer);
        }
        _feed_timer = setTimeout(lambda { load_feed(); }, 400);
    }

    """Fetch whatever was published while the stream was down."""
    async def backfill {
        missed = await get_json("/notifications?since=" + str(_last_id));
        for n in (missed or []) {
            handle(n as Notification, True);
        }
    }

    """SSE with reconnect: EventSource retries by itself, and every (re)open
    runs the backfill."""
    def connect {
        # the checker has no browser typings for EventSource
        _stream = new(EventSource, "/stream");  # jac:ignore[E1053]
        _stream.addEventListener(
            "notification",
            lambda (ev: any) { handle(JSON.parse(ev.data) as Notification, True); }
        );
        _stream.onopen = lambda { backfill(); };
    }

    async def boot {
        # Seed the seen-set with history so old notifications don't flood the tray.
        hist = await get_json("/notifications?since=0");
        for n in (hist or []) {
            handle(n as Notification, False);
        }
        connect();
        load_feed();
        load_status();
    }

    def on_key(e: KeyboardEvent) {
        if e.key == "Escape" and len(tray) > 0 {
            ack(tray[0].notification_id);
        }
    }

    async def toggle_suppressed(e: ChangeEvent) {
        showSuppressed = e.target.checked;
        if not showSuppressed {
            return;
        }
        data = await get_json("/suppressed");
        suppressed = (data.posts if data else []) as list[SuppressedPost];
    }

    def toggle_sound(e: ChangeEvent) {
        sound = e.target.checked;
        localStorage.setItem("ns_sound", "1" if sound else "0");
    }

    async def do_refetch {
        refetch = "running";
        ok = False;
        try {
            resp = await fetch("/refetch?hours=12", {"method": "POST"});
            ok = resp.ok;
        } except Exception {}
        if not ok {
            refetch = "failed";
            setTimeout(lambda { refetch = "idle"; }, 4000);
        }
        # On success, load_status puts the button back when the run finishes.
    }

    # Runs once when the page loads; the returned function undoes it.
    useEffect(
        lambda {
            sound = localStorage.getItem("ns_sound") == "1";
            boot();
            status_timer = setInterval(lambda { load_status(); }, 10000);
            clock_timer = setInterval(lambda { clock = clock + 1; }, 30000);
            document.addEventListener("keydown", on_key);
            return lambda {
                clearInterval(status_timer);
                clearInterval(clock_timer);
                document.removeEventListener("keydown", on_key);
                if _stream {
                    _stream.close();
                }
            };
        },
        []
    );

    <div className={"has-banner" if banner else ""} data-clock={clock}>
        {if banner {
            <div id="banner">{banner}</div>
        }}
        <div id="tray">
            {for n in visible {
                <TrayCard key={n.notification_id} n={n} onAck={ack}/>
            }}
            {if hidden > 0 {
                <div id="morepill" onClick={lambda { expanded = True; }}>
                    {"+" + str(hidden) + " more"}
                </div>
            }}
        </div>
        <main>
            <h1>newsstream — crude oil</h1>
            <div className="controls">
                <label>
                    <input
                        type="checkbox"
                        checked={showSuppressed}
                        onChange={toggle_suppressed}
                    />
                    {" show suppressed (threshold tuning)"}
                </label>
                <label>
                    <input type="checkbox" checked={sound} onChange={toggle_sound}/>
                    {" chime on new events"}
                </label>
                <button
                    title="Re-pull the last 12h from every account and run it through the pipeline — use after waking the machine"
                    disabled={refetch != "idle"}
                    onClick={lambda { do_refetch(); }}
                >
                    {refetch_label}
                </button>
            </div>
            {if showSuppressed {
                <div id="suppressed">
                    {for p in suppressed {
                        <SuppressedRow key={p.id} p={p}/>
                    }}
                    {if len(suppressed) == 0 {
                        <div className="dim">Nothing suppressed yet.</div>
                    }}
                </div>
            }}
            <div id="feed">
                {for ev in events {
                    <EventBlock key={ev.id} ev={ev}/>
                }}
                {if feedLoaded and len(events) == 0 {
                    <div className="dim">No events yet.</div>
                }}
            </div>
        </main>
        <Footer s={status} unreachable={unreachable}/>
    </div>
}
