"""Notification cards + in-process SSE bus (CLAUDE.md §6 step 7, §10)."""

import asyncio;
import contextlib;
import logging;
import from adapters.base { Json, NormalizedPost }
import from db.repo { Repo, iso }
import from pipeline.classify { Classification }

glob log: logging.Logger = logging.getLogger("newsstream.notify");

"""Fan-out of notification payloads to connected SSE clients."""
obj Bus {
    has _subscribers: set[asyncio.Queue[Json]] postinit;

    def postinit {
        self._subscribers = set();
    }

    def publish(payload: Json) {
        for q in list(self._subscribers) {
            q.put_nowait(payload);
        }
    }

    @contextlib.asynccontextmanager
    async def subscribe -> AsyncIterator[asyncio.Queue[Json]] {
        q: asyncio.Queue[Json] = asyncio.Queue();
        self._subscribers.add(q);
        try {
            yield q;
        } finally {
            self._subscribers.discard(q);
        }
    }
}

def build_card(
    kind: str,
    post: NormalizedPost,
    post_id: int,
    cls: Classification | None,
    *,
    event_id: int | None = None,
    event_title: str = "",
    what_changed: str = "",
    badges: list[str] | None = None
) -> Json {
    platform_latency = (post.received_at - post.posted_at).total_seconds();
    return {
        "kind": kind,
        "post_id": post_id,
        "category": cls.category if cls else None,
        "one_line": (cls.one_line if cls else "") or post.text[:140],
        "score": cls.materiality_score if cls else None,
        "handle": post.account_handle,
        "tier": post.tier,
        "url": post.url,
        "posted_at": iso(post.posted_at),
        "received_at": iso(post.received_at),
        "platform_latency_s": round(platform_latency, 1),
        "event_id": event_id,
        "event_title": event_title,
        "what_changed": what_changed,
        "badges": badges or []
    };
}

async def notify(repo: Repo, bus: Bus, card: Json) -> int {
    nid = await repo.insert_notification(
        str(card["kind"]), int(card["post_id"]), card.get("event_id"), card
    );
    card["notification_id"] = nid;
    bus.publish(card);
    log.info(
        "surfaced | kind=%s post=%s event=%s",
        card["kind"],
        card["post_id"],
        card.get("event_id")
    );
    return nid;
}
