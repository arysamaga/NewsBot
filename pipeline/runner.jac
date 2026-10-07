"""Pipeline orchestration (CLAUDE.md §6): persist -> classify -> dedup -> notify.

Operational rules honored here (§12):
- Persist raw before any LLM call; never lose a post to a downstream failure.
- 3+ consecutive Anthropic errors -> fail open: official/tracker posts surface
  unclassified with an "unfiltered" badge until calls succeed again.
- One INFO line per post: received | classified | dedup | latency.
- Dedup candidates are fetched concurrently with classification (latency §6).
"""

import asyncio;
import logging;
import time;
import from collections { deque }
import from adapters.base { NormalizedPost }
import from config { Settings }
import from db.repo { Repo, Row }
import from pipeline.classify { Classification, classify }
import from pipeline.dedup {
    DedupResult,
    decide,
    fetch_open_events,
    filter_candidates,
    merge_summary,
    merged_entities
}
import from pipeline.llm { LLMClient, LLMUnavailable }
import from pipeline.normalize { normalize }
import from pipeline.notify { Bus, build_card, notify }

glob log: logging.Logger = logging.getLogger("newsstream.pipeline");

# (timestamp, seconds) samples
type Samples = deque[tuple[float, float]];

"""Rolling latency metrics for the UI footer."""
obj Stats {
    has platform: Samples postinit,  # posted -> received
        surface: Samples postinit;  # received -> published

    def postinit {
        self.platform = deque(maxlen=2000);
        self.surface = deque(maxlen=2000);
    }

    static def _percentiles(samples: Samples) -> dict[str, float | None] {
        cutoff = time.time() - 3600;
        vals = sorted(
            v
            for (ts, v) in samples
            if ts >= cutoff
        );
        if not vals {
            return {"p50": None, "p95": None};
        }
        return {
            "p50": round(vals[len(vals) // 2], 1),
            "p95": round(vals[min(len(vals) - 1, int(len(vals) * 0.95))], 1)
        };
    }

    def snapshot -> dict[str, dict[str, float | None]] {
        return {
            "platform": Stats._percentiles(self.platform),
            "surface": Stats._percentiles(self.surface)
        };
    }
}

obj Pipeline {
    has settings: Settings,
        repo: Repo,
        llm: LLMClient,
        bus: Bus;

    has stats: Stats postinit,
        queue: asyncio.Queue[NormalizedPost] postinit;

    def postinit {
        self.stats = Stats();
        self.queue = asyncio.Queue();
    }

    """Adapter-facing entry point: enqueue, never block ingest on the LLM."""
    async def sink(post: NormalizedPost) {
        self.queue.put_nowait(post);
    }

    async def run {
        while True {
            post = await self.queue.get();
            try {
                await self.process(post);
            } except Exception {
                log.exception(
                    "pipeline failure for %s:%s (post persisted, continuing)",
                    post.source,
                    post.source_post_id
                );
            }
        }
    }

    async def process(raw_post: NormalizedPost) -> str {
        post = normalize(raw_post, self.settings);
        if post is None {
            return "dropped";
        }

        post_id = await self.repo.insert_post(post);
        if post_id is None {
            log.debug("already seen %s:%s", post.source, post.source_post_id);
            return "seen";
        }

        t0 = time.monotonic();
        self.stats.platform.append(
            (time.time(), (post.received_at - post.posted_at).total_seconds())
        );

        # Candidate prefetch runs while classification is in flight.
        events_task = asyncio.create_task(fetch_open_events(self.repo, self.settings));
        classifications: list[Classification] = [];
        try {
            classifications = await classify(self.llm, self.settings, post, post_id);
        } except LLMUnavailable {
            events_task.cancel();
            return await self._handle_llm_down(post, post_id, t0);
        }

        for (i, c) in enumerate(classifications) {
            await self.repo.insert_decision(
                post_id,
                c.stage,
                c.model,
                materiality_score=c.materiality_score,
                sub_scores=c.sub_scores,
                category=c.category,
                one_line=c.one_line,
                entities=c.entities,
                reasoning=c.reasoning,
                error=c.error,
                is_final=(i == len(classifications) - 1)
            );
        }
        final = classifications[-1];

        outcome = "suppressed";
        if final.materiality_score < self.settings.market.threshold {
            events_task.cancel();
        } else {
            outcome = await self._surface(post, post_id, final, events_task);
        }

        elapsed = time.monotonic() - t0;
        if outcome in ("NEW_EVENT", "UPDATE") {
            self.stats.surface.append((time.time(), elapsed));
        }
        log.info(
            "received %s:%s | classified score=%d cat=%s | dedup=%s | latency=%.1fs",
            post.source,
            post.source_post_id,
            final.materiality_score,
            final.category,
            outcome,
            elapsed
        );
        return outcome;
    }

    async def _surface(
        post: NormalizedPost,
        post_id: int,
        final: Classification,
        events_task: asyncio.Task[list[Row]]
    ) -> str {
        open_events = await events_task;
        candidates = filter_candidates(open_events, final);
        result: DedupResult;
        try {
            result = await decide(
                self.llm, self.settings, post, final, candidates, post_id
            );
        } except LLMUnavailable {
            # Classification succeeded but dedup could not run: surfacing a
            # possible duplicate beats going dark.
            result = DedupResult("NEW_EVENT", None, reasoning="dedup_llm_unavailable");
        }

        badges = ["classifier-error"] if final.error else [];

        if result.decision == "NEW_EVENT" {
            title = final.one_line or post.text[:120];
            new_id = await self.repo.create_event(
                final.category, title, title, final.entities
            );
            await self.repo.link_event_post(new_id, post_id, "origin");
            await notify(
                self.repo,
                self.bus,
                build_card(
                    "NEW_EVENT",
                    post,
                    post_id,
                    final,
                    event_id=new_id,
                    event_title=title,
                    badges=badges
                )
            );
            return "NEW_EVENT";
        }

        # decide() always names a candidate event for UPDATE / DUPLICATE.
        event_id = result.event_id;
        assert event_id is not None , "dedup decision without an event id";
        event = await self.repo.get_event(event_id);
        if result.decision == "UPDATE" {
            assert event is not None , f"event {event_id} not found";
            summary = await merge_summary(
                self.llm, self.settings, event, post, result, post_id
            );
            entities = merged_entities(event["entities"], final.entities);
            await self.repo.update_event(event_id, summary, entities);
            await self.repo.link_event_post(event_id, post_id, "update");
            await notify(
                self.repo,
                self.bus,
                build_card(
                    "UPDATE",
                    post,
                    post_id,
                    final,
                    event_id=event_id,
                    event_title=str(event["title"]),
                    what_changed=result.what_changed,
                    badges=badges
                )
            );
            return "UPDATE";
        }

        await self.repo.link_event_post(event_id, post_id, "duplicate");
        return "DUPLICATE";
    }

    async def _handle_llm_down(post: NormalizedPost, post_id: int, t0: float) -> str {
        await self.repo.insert_decision(
            post_id, "unfiltered", "", error="llm_unavailable", is_final=True
        );
        outcome = "held-llm-down";
        if self.llm.fail_open and post.tier in ("official", "tracker") {
            await notify(
                self.repo,
                self.bus,
                build_card("UNFILTERED", post, post_id, None, badges=["unfiltered"])
            );
            outcome = "UNFILTERED";
        }
        log.warning(
            "received %s:%s | classify unavailable (consecutive_errors=%d) | %s | latency=%.1fs",
            post.source,
            post.source_post_id,
            self.llm.consecutive_errors,
            outcome,
            time.monotonic() - t0
        );
        return outcome;
    }

    """Mark events stale after stale_after_hours with no updates (§8)."""
    async def stale_sweeper {
        while True {
            n = await self.repo.mark_stale_events(self.settings.stale_after_hours);
            if n {
                log.info("marked %d events stale", n);
            }
            await asyncio.sleep(600);
        }
    }
}
