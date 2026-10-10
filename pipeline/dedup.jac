"""Event deduplication / clustering (CLAUDE.md §8).

"Already surfaced" is a list of events, not posts. Candidates are open events
touched within the stale window with the same category OR overlapping entities,
capped at 8, most recent first. One review-model call decides NEW_EVENT /
UPDATE / DUPLICATE. No embeddings in v1 (volume is tens of posts/day).
"""

import json;
import logging;
import re;
import from datetime { timedelta }
import from pathlib { Path }
import from adapters.base { Json, NormalizedPost }
import from config { Settings }
import from db.repo { Repo, Row, utcnow }
import from pipeline.classify { Classification }
import from pipeline.llm { LLMClient, LLMParseError }

glob log: logging.Logger = logging.getLogger("newsstream.dedup");

glob PROMPT_PATH: Path = Path(__file__).parent / "prompts" / "dedup_crude.md",
     MAX_CANDIDATES: int = 8,
     DEDUP_MAX_TOKENS: int = 1000,
     SUMMARY_MAX_TOKENS: int = 800;

obj DedupResult {
    has decision: str,  # NEW_EVENT | UPDATE | DUPLICATE
        event_id: int | None,
        what_changed: str = "",
        reasoning: str = "";
}

"""Event entities arrive as a JSON string from the DB or as a dict from callers."""
def _as_entities(raw: any) -> Json {
    return json.loads(raw) if isinstance(raw, str) else raw;
}

def _entity_set(entities: Json) -> set[str] {
    out: set[str] = set();
    for key in ("vessels", "locations", "actors") {
        out |= {
            str(x).strip().lower()
            for x in (entities.get(key) or [])
            if str(x).strip()
        };
    }
    return out;
}

def _tokens(s: str) -> set[str] {
    return {
        t
        for t in re.split(r"[^a-z0-9]+", s)
        if len(t) >= 3
    };
}

"""Fuzzy: exact match, substring (>=4 chars), or a shared token.
Errs loose on purpose — a false candidate just goes to the dedup LLM,
while a missed candidate splits one event in two ("Jazan" must match
"Jazan refinery")."""
def entities_overlap(a: set[str], b: set[str]) -> bool {
    if a & b {
        return True;
    }
    for x in a {
        for y in b {
            if (len(x) >= 4 and x in y) or (len(y) >= 4 and y in x) {
                return True;
            }
            if _tokens(x) & _tokens(y) {
                return True;
            }
        }
    }
    return False;
}

"""Same category OR overlapping entities; input is already newest-first."""
def filter_candidates(open_events: list[Row], cls: Classification) -> list[Row] {
    post_entities = _entity_set(cls.entities);
    picked: list[Row] = [];
    for ev in open_events {
        ev_entities = _entity_set(_as_entities(ev["entities"]));
        if ev["category"] == cls.category
            or (post_entities and entities_overlap(post_entities, ev_entities)) {
            picked.append(ev);
        }
        if len(picked) >= MAX_CANDIDATES {
            break;
        }
    }
    return picked;
}

async def fetch_open_events(repo: Repo, settings: Settings) -> list[Row] {
    cutoff = utcnow() - timedelta(hours=settings.stale_after_hours);
    return await repo.open_events_since(cutoff);
}

def _build_user(
    post: NormalizedPost, cls: Classification, candidates: list[Row]
) -> str {
    cand_lines: list[str] = [];
    for ev in candidates {
        entities = _as_entities(ev["entities"]);
        cand_lines.append(
            f"- id {ev['id']} [{ev['category']}] {ev['title']}\n"
            f"  summary: {ev['summary']}\n"
            f"  entities: {json.dumps(entities, sort_keys=True)}"
        );
    }
    parts = [
        f"Post (@{post.account_handle}, {post.posted_at.isoformat()}):\n{post.text}"
    ];
    if post.quoted_text {
        parts.append(f"Quoted post:\n{post.quoted_text}");
    }
    parts.append(
        f"Classifier read: [{cls.category}] {cls.one_line}\n"
        f"entities: {json.dumps(cls.entities, sort_keys=True)}"
    );
    parts.append("Candidate events:\n" + "\n".join(cand_lines));
    return "\n\n".join(parts);
}

"""Raises LLMUnavailable; a parse failure fails open to NEW_EVENT."""
async def decide(
    llm: LLMClient,
    settings: Settings,
    post: NormalizedPost,
    cls: Classification,
    candidates: list[Row],
    post_id: int | None
) -> DedupResult {
    if not candidates {
        return DedupResult("NEW_EVENT", None, reasoning="no open candidate events");
    }

    system = PROMPT_PATH.read_text();
    user = _build_user(post, cls, candidates);
    raw: Json = {};
    try {
        raw = await llm.call_json(
            "dedup",
            settings.review_model,
            system,
            user,
            max_tokens=DEDUP_MAX_TOKENS,
            post_id=post_id,
            prefer={"output_config": {"effort": "low"}}
        );
    } except LLMParseError {
        log.warning(
            "dedup parse failure, failing open to NEW_EVENT | post=%s", post_id
        );
        return DedupResult("NEW_EVENT", None, reasoning="dedup_parse_error");
    }

    decision = raw.get("decision");
    if decision not in ("NEW_EVENT", "UPDATE", "DUPLICATE") {
        return DedupResult("NEW_EVENT", None, reasoning="invalid dedup decision");
    }
    if decision == "NEW_EVENT" {
        return DedupResult(
            "NEW_EVENT", None, reasoning=str(raw.get("reasoning") or "")
        );
    }

    cand_ids = {int(ev["id"]) for ev in candidates};
    event_id = -1;
    try {
        event_id = int(raw.get("event_id"));
    } except (TypeError, ValueError) {
        event_id = -1;
    }
    if event_id not in cand_ids {
        log.warning(
            "dedup returned unknown event_id %s; clamping to newest candidate",
            raw.get("event_id")
        );
        event_id = int(candidates[0]["id"]);
    }
    return DedupResult(
        str(decision),
        event_id,
        what_changed=str(raw.get("what_changed") or "")[:200],
        reasoning=str(raw.get("reasoning") or "")[:400]
    );
}

def merged_entities(
    event_entities: Json | str, post_entities: Json
) -> dict[str, list[str]] {
    ev = _as_entities(event_entities);
    out: dict[str, list[str]] = {};
    for key in ("vessels", "locations", "actors") {
        seen: dict[str, str] = {};
        for x in list(ev.get(key) or []) + list(post_entities.get(key) or []) {
            s = str(x).strip();
            if s and s.lower() not in seen {
                seen[s.lower()] = s;
            }
        }
        out[key] = list(seen.values());
    }
    return out;
}

"""Rewrite the event summary (<= 60 words) folding in the new fact.
Any failure falls back to appending what_changed, never blocks the update."""
async def merge_summary(
    llm: LLMClient,
    settings: Settings,
    event: Row,
    post: NormalizedPost,
    result: DedupResult,
    post_id: int | None
) -> str {
    system = (
        "You maintain one-paragraph summaries of oil-market events. Merge the new "
        "post into the existing summary. Max 60 words. Return ONLY JSON: "
        '{"summary": "..."}'
    );
    user = (
        f"Existing summary:\n{event['summary']}\n\n"
        f"New post (@{post.account_handle}):\n{post.text}\n\n"
        f"What changed: {result.what_changed or 'see post'}"
    );
    try {
        raw = await llm.call_json(
            "summary_merge",
            settings.review_model,
            system,
            user,
            max_tokens=SUMMARY_MAX_TOKENS,
            post_id=post_id,
            prefer={"output_config": {"effort": "low"}}
        );
        summary = str(raw.get("summary") or "").strip();
        if summary {
            return summary;
        }
    } except Exception as e {
        # summary merge is best-effort by design
        log.warning("summary merge failed (%s); appending what_changed", e);
    }
    return f"{event['summary']} Update: {result.what_changed or post.text[:100]}";
}
