"""SourceAdapter base + NormalizedPost (CLAUDE.md §4).

Rules every adapter must obey:
- Track by numeric account ID; refresh the handle from the ID; an ID that stops
  resolving marks the account inactive (never crash).
- Drop replies at the adapter: is_reply=True never enters the pipeline.
- Quote tweets are top-level posts with quoted_text populated.
- Record received_at; posted_at - received_at is the latency metric.
"""

import from datetime { datetime }

obj NormalizedPost {
    has source: str,  # "telegram" | "x"
        source_post_id: str,
        account_id: str,  # STABLE numeric id, never the handle
        account_handle: str,  # display only; may change
        url: str,
        text: str,
        quoted_text: str | None,
        is_reply: bool,
        posted_at: datetime,  # UTC, from the platform
        received_at: datetime,  # UTC, when we got it
        media_urls: list[str] = [],
        # Resolved from config by the pipeline (source tier is metadata only, §5);
        # adapters may leave the default.
        tier: str = "osint";
}

# A JSON object as delivered by a platform API; values vary by key.
type Json = dict[str, any];

# An async callable taking one post. The checker models an `async def` by its
# awaited result, so the alias is spelled with the awaited type.
type Sink = Callable[[NormalizedPost], None];

obj AdapterStatus {
    has name: str,
        # live | reconnecting | budget-paused | disabled | error | starting
        state: str = "starting",
        detail: str = "";
}

obj SourceAdapter {
    has status: AdapterStatus postinit;

    def postinit {
        self.status = AdapterStatus(name="adapter");
    }

    """Long-running task: push every new top-level post into sink."""
    async def run(sink: Sink) -> None abst;

    """handle -> (stable id, canonical handle)."""
    async def resolve_account(handle: str) -> tuple[str, str] abst;
}
