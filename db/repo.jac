"""SQLite repository. One aiosqlite connection, WAL mode, all times UTC ISO8601."""

import json;
import aiosqlite;
import from datetime { UTC, datetime, timedelta }
import from pathlib { Path }
import from adapters.base { NormalizedPost }

# A database row (or JSON object) keyed by column name; values vary by column.
type Row = dict[str, any];
type Params = Sequence[any];

glob SCHEMA: str = (Path(__file__).parent / "schema.sql").read_text();

def utcnow -> datetime {
    return datetime.now(UTC);
}

"""Normalize to aware-UTC ISO8601 so string comparison == time comparison."""
def iso(dt: datetime) -> str {
    if dt.tzinfo is None {
        dt = dt.replace(tzinfo=UTC);
    }
    return dt.astimezone(UTC).isoformat();
}

obj Repo {
    has db_path: Path | str;
    has _db: aiosqlite.Connection | None = None;
    has db: aiosqlite.Connection {
        getter -> aiosqlite.Connection {
            assert self._db is not None , "Repo.connect() not called";
            return self._db;
        }
    }

    async def connect {
        path = Path(self.db_path);
        path.parent.mkdir(parents=True, exist_ok=True);
        self._db = await aiosqlite.connect(path);
        self.db.row_factory = aiosqlite.Row;
        await self.db.execute("PRAGMA busy_timeout = 5000");
        await self.db.executescript(SCHEMA);
        await self.db.commit();
    }

    async def close {
        if self._db is not None {
            await self._db.close();
            self._db = None;
        }
    }

    """Run one statement; typed wrapper over aiosqlite's awaitable cursor."""
    async def _run(sql: str, args: Params = ()) -> aiosqlite.Cursor {
        return (await self.db.execute(sql, args)) as aiosqlite.Cursor;
    }

    async def _all(sql: str, args: Params = ()) -> list[Row] {
        rows = await (await self._run(sql, args)).fetchall();
        return [dict(r) for r in rows];
    }

    async def _one(sql: str, args: Params = ()) -> Row | None {
        row = await (await self._run(sql, args)).fetchone();
        return dict(row) if row else None;
    }

    # -- accounts -------------------------------------------------------------

    async def upsert_account(source: str, account_id: str, handle: str, tier: str) {
        await self._run(
            """INSERT INTO accounts (source, account_id, handle, tier, status, created_at, last_seen_at)
               VALUES (?, ?, ?, ?, 'active', ?, ?)
               ON CONFLICT (source, account_id)
               DO UPDATE SET handle = excluded.handle, tier = excluded.tier,
                             status = 'active', last_seen_at = excluded.last_seen_at""",
            (source, account_id, handle, tier, iso(utcnow()), iso(utcnow()))
        );
        await self.db.commit();
    }

    async def set_account_status(source: str, account_id: str, status: str) {
        await self._run(
            "UPDATE accounts SET status = ? WHERE source = ? AND account_id = ?",
            (status, source, account_id)
        );
        await self.db.commit();
    }

    async def get_accounts(source: str | None = None) -> list[Row] {
        q = "SELECT * FROM accounts";
        args: Params = ();
        if source {
            q += " WHERE source = ?";
            args = (source, );
        }
        return await self._all(q, args);
    }

    # -- posts ----------------------------------------------------------------

    """Persist a NormalizedPost. Returns row id, or None if already seen."""
    async def insert_post(p: NormalizedPost) -> int | None {
        try {
            cur = await self._run(
                """INSERT INTO posts (source, source_post_id, account_id, account_handle, tier,
                                      url, text, quoted_text, is_reply, posted_at, received_at, media_urls)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    p.source,
                    p.source_post_id,
                    p.account_id,
                    p.account_handle,
                    p.tier,
                    p.url,
                    p.text,
                    p.quoted_text,
                    int(p.is_reply),
                    iso(p.posted_at),
                    iso(p.received_at),
                    json.dumps(p.media_urls)
                )
            );
        } except aiosqlite.IntegrityError {
            return None;
        }
        await self.db.commit();
        return cur.lastrowid;
    }

    async def get_post(post_id: int) -> Row | None {
        return await self._one("SELECT * FROM posts WHERE id = ?", (post_id, ));
    }

    # -- decisions ------------------------------------------------------------

    async def insert_decision(
        post_id: int,
        stage: str,
        model: str,
        *,
        materiality_score: int | None = None,
        sub_scores: dict[str, int] | None = None,
        category: str | None = None,
        one_line: str | None = None,
        entities: dict[str, list[str]] | None = None,
        reasoning: str | None = None,
        error: str | None = None,
        is_final: bool = False
    ) -> int {
        cur = await self._run(
            """INSERT INTO decisions (post_id, stage, model, materiality_score, sub_scores,
                                      category, one_line, entities, reasoning, error, is_final, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                post_id,
                stage,
                model,
                materiality_score,
                json.dumps(sub_scores) if sub_scores is not None else None,
                category,
                one_line,
                json.dumps(entities) if entities is not None else None,
                reasoning,
                error,
                int(is_final),
                iso(utcnow())
            )
        );
        await self.db.commit();
        return cur.lastrowid or 0;
    }

    # -- events ---------------------------------------------------------------

    async def create_event(
        category: str, title: str, summary: str, entities: dict[str, list[str]]
    ) -> int {
        now = iso(utcnow());
        cur = await self._run(
            """INSERT INTO events (category, title, summary, entities, status, first_seen_at, last_updated_at)
               VALUES (?, ?, ?, ?, 'open', ?, ?)""",
            (category, title, summary, json.dumps(entities), now, now)
        );
        await self.db.commit();
        return cur.lastrowid or 0;
    }

    async def get_event(event_id: int) -> Row | None {
        return await self._one("SELECT * FROM events WHERE id = ?", (event_id, ));
    }

    async def open_events_since(cutoff: datetime) -> list[Row] {
        return await self._all(
            """SELECT * FROM events WHERE status = 'open' AND last_updated_at >= ?
               ORDER BY last_updated_at DESC""",
            (iso(cutoff), )
        );
    }

    async def update_event(
        event_id: int, summary: str, entities: dict[str, list[str]] | None = None
    ) {
        if entities is not None {
            await self._run(
                "UPDATE events SET summary = ?, entities = ?, last_updated_at = ? WHERE id = ?",
                (summary, json.dumps(entities), iso(utcnow()), event_id)
            );
        } else {
            await self._run(
                "UPDATE events SET summary = ?, last_updated_at = ? WHERE id = ?",
                (summary, iso(utcnow()), event_id)
            );
        }
        await self.db.commit();
    }

    async def mark_stale_events(older_than_hours: int) -> int {
        cutoff = iso(utcnow() - timedelta(hours=older_than_hours));
        cur = await self._run(
            "UPDATE events SET status = 'stale' WHERE status = 'open' AND last_updated_at < ?",
            (cutoff, )
        );
        await self.db.commit();
        return cur.rowcount;
    }

    async def link_event_post(event_id: int, post_id: int, role: str) {
        await self._run(
            "INSERT OR IGNORE INTO event_posts (event_id, post_id, role) VALUES (?, ?, ?)",
            (event_id, post_id, role)
        );
        await self.db.commit();
    }

    # -- notifications --------------------------------------------------------

    async def insert_notification(
        kind: str, post_id: int, event_id: int | None, payload: Row
    ) -> int {
        cur = await self._run(
            "INSERT INTO notifications (kind, post_id, event_id, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (kind, post_id, event_id, json.dumps(payload), iso(utcnow()))
        );
        await self.db.commit();
        return cur.lastrowid or 0;
    }

    async def notifications_since(since_id: int, limit: int = 200) -> list[Row] {
        rows = await self._all(
            "SELECT id, kind, payload FROM notifications WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since_id, limit)
        );
        out: list[Row] = [];
        for r in rows {
            payload = json.loads(r["payload"]);
            payload["notification_id"] = r["id"];
            out.append(payload);
        }
        return out;
    }

    # -- llm accounting -------------------------------------------------------

    async def insert_llm_call(
        purpose: str,
        model: str,
        post_id: int | None,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int
    ) {
        await self._run(
            """INSERT INTO llm_calls (purpose, model, post_id, input_tokens, output_tokens, latency_ms, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                purpose,
                model,
                post_id,
                input_tokens,
                output_tokens,
                latency_ms,
                iso(utcnow())
            )
        );
        await self.db.commit();
    }

    async def llm_usage_today -> list[Row] {
        day_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0);
        return await self._all(
            """SELECT model, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens
               FROM llm_calls WHERE created_at >= ? GROUP BY model""",
            (iso(day_start), )
        );
    }

    # -- x read budget --------------------------------------------------------

    """Add n reads for today (UTC); returns the new daily total."""
    async def add_x_reads(n: int) -> int {
        day = utcnow().strftime("%Y-%m-%d");
        await self._run(
            """INSERT INTO x_reads (day, reads) VALUES (?, ?)
               ON CONFLICT (day) DO UPDATE SET reads = reads + excluded.reads""",
            (day, n)
        );
        await self.db.commit();
        return await self.x_reads_today();
    }

    async def x_reads_today -> int {
        day = utcnow().strftime("%Y-%m-%d");
        row = await self._one("SELECT reads FROM x_reads WHERE day = ?", (day, ));
        return int(row["reads"]) if row else 0;
    }

    # -- UI queries -----------------------------------------------------------

    """Events with surfaced posts nested. Active (open) events first,
    stale ones below them regardless of age; within each group, newest
    origin post first (the story is as recent as the tweet that first
    created it)."""
    async def feed(limit: int = 100) -> list[Row] {
        events = await self._all(
            """SELECT e.*, COALESCE(
                     (SELECT MIN(p.posted_at) FROM event_posts ep
                      JOIN posts p ON p.id = ep.post_id
                      WHERE ep.event_id = e.id AND ep.role = 'origin'),
                     e.first_seen_at) AS origin_posted_at
               FROM events e
               ORDER BY (e.status != 'open'), origin_posted_at DESC LIMIT ?""",
            (limit, )
        );
        for ev in events {
            ev["entities"] = json.loads(ev["entities"]);
            ev["posts"] = await self._all(
                """SELECT p.*, ep.role, d.one_line, d.category, d.materiality_score
                   FROM event_posts ep
                   JOIN posts p ON p.id = ep.post_id
                   LEFT JOIN decisions d ON d.post_id = p.id AND d.is_final = 1
                   WHERE ep.event_id = ? ORDER BY p.received_at ASC""",
                (ev["id"], )
            );
        }
        return events;
    }

    """Posts whose final decision landed below threshold, with scores for tuning."""
    async def suppressed(limit: int = 200) -> list[Row] {
        return await self._all(
            """SELECT p.id, p.account_handle, p.tier, p.url, p.text, p.posted_at, p.received_at,
                      d.materiality_score, d.sub_scores, d.category, d.one_line, d.error
               FROM posts p JOIN decisions d ON d.post_id = p.id AND d.is_final = 1
               WHERE p.id NOT IN (SELECT post_id FROM event_posts)
                 AND p.id NOT IN (SELECT post_id FROM notifications)
               ORDER BY p.received_at DESC LIMIT ?""",
            (limit, )
        );
    }

    async def counts_today -> dict[str, int] {
        day_start = iso(utcnow().replace(hour=0, minute=0, second=0, microsecond=0));
        seen = await self._one(
            "SELECT COUNT(*) AS n FROM posts WHERE received_at >= ?", (day_start, )
        );
        surfaced = await self._one(
            "SELECT COUNT(*) AS n FROM notifications WHERE created_at >= ?",
            (day_start, )
        );
        return {
            "posts_seen": int(seen["n"]) if seen else 0,
            "surfaced": int(surfaced["n"]) if surfaced else 0
        };
    }
}
