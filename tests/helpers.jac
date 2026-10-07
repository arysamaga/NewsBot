"""Shared test helpers.

Two LLM strategies (CLAUDE.md §11):
- StubLLM: scripted responses for pipeline-logic tests; always runs offline.
- real_llm: LLMClient backed by tests/cassettes/llm.json. With
  ANTHROPIC_API_KEY set, the first run records the cassette; afterwards the
  model-behavior tests run offline and deterministically. Without key or
  cassette they skip with a clear message. Record in ONE full serial run so
  the dedup sequences (whose prompts chain on earlier recorded outputs) stay
  consistent: ANTHROPIC_API_KEY=... JAC_TEST_JOBS=0 jac test
"""

import contextlib;
import json;
import os;
import tempfile;
import from datetime { datetime, timedelta }
import from pathlib { Path }
import from dotenv { load_dotenv }
import from adapters.base { Json, NormalizedPost }
import from config { ROOT, MarketCfg, Settings, load_settings }
import from db.repo { Repo }
import from pipeline.llm { Cassette, LLMClient, LLMParseError, LLMUnavailable }

glob FIXTURES_DIR: Path = Path(__file__).parent / "fixtures",
     CASSETTE_PATH: Path = Path(__file__).parent / "cassettes" / "llm.json";

# so a plain `jac test` can record cassettes
glob _ENV_LOADED: bool = load_dotenv(str(ROOT / ".env"));

glob HAVE_KEY: bool = bool(os.environ.get("ANTHROPIC_API_KEY")),
     HAVE_CASSETTE: bool = CASSETTE_PATH.exists();

"""Skip the calling test unless the model can be reached or replayed."""
def requires_llm {
    if not (HAVE_KEY or HAVE_CASSETTE) {
        testskip(
            "needs ANTHROPIC_API_KEY (records cassette) or tests/cassettes/llm.json (replays)"
        );
    }
}

"""Real config and a fresh temp database, with the market threshold pinned to
the spec default: fixture expectations (and recorded cassettes) are calibrated
to 70, and the user tuning config.yaml must not break the suite."""
def make_settings -> Settings {
    tmp = Path(tempfile.mkdtemp(prefix="newsstream-test-"));
    s = load_settings(config_path=ROOT / "config.yaml", db_path=tmp / "test.db");
    s.market = MarketCfg(
        name=s.market.name,
        threshold=70,
        review_band=8,
        rubric_weights=s.market.rubric_weights
    );
    return s;
}

"""A connected Repo on the settings' database, closed on exit."""
@contextlib.asynccontextmanager
async def open_repo(settings: Settings) -> AsyncIterator[Repo] {
    r = Repo(settings.db_path);
    await r.connect();
    try {
        yield r;
    } finally {
        await r.close();
    }
}

def real_llm(repo: Repo) -> LLMClient {
    mode = "auto" if HAVE_KEY else "replay";
    return LLMClient(
        os.environ.get("ANTHROPIC_API_KEY", ""),
        repo,
        cassette=Cassette(CASSETTE_PATH, mode)
    );
}

"""Stand-in LLMClient: pops scripted responses; exceptions raise."""
obj StubLLM(LLMClient) {
    has responses: list[any] = [],
        calls: list[dict[str, str]] = [];

    override async def call_json(
        purpose: str,
        model: str,
        system: str,
        user: str,
        *,
        max_tokens: int = 400,
        post_id: int | None = None,
        prefer: Json | None = None
    ) -> Json {
        self.calls.append({"purpose": purpose, "model": model, "user": user});
        item = self.responses.pop(0) if self.responses else {};
        if isinstance(item, Exception) {
            if isinstance(item, LLMUnavailable)
                and not isinstance(item, LLMParseError) {
                self.consecutive_errors += 1;
            }
            raise item;
        }
        self.consecutive_errors = 0;
        return item;
    }
}

"""A StubLLM scripted with the given responses (dicts or exceptions)."""
def stub_llm(responses: list[any] | None = None) -> StubLLM {
    return StubLLM(api_key="", responses=list(responses or []));
}

def sub_scores(
    e: int = 0, f: int = 0, p: int = 0, s: int = 0, n: int = 0
) -> dict[str, int] {
    return {
        "event_not_commentary": e,
        "flow_impact": f,
        "primary_source": p,
        "specificity": s,
        "novelty_prior": n
    };
}

def classify_response(
    score_subs: dict[str, int],
    category: str = "STRIKE_MILITARY",
    one_line: str = "something happened",
    entities: Json | None = None
) -> Json {
    return {
        "sub_scores": score_subs,
        "category": category,
        "one_line": one_line,
        "entities": entities or {"vessels": [], "locations": [], "actors": []},
        "reasoning": "stub"
    };
}

"""Fixture JSON entry -> (NormalizedPost, expectations).
received_at is posted_at + 30s so prompts (and cassette keys) are stable."""
def fixture_post(d: Json) -> tuple[NormalizedPost, Json] {
    posted = datetime.fromisoformat(d["posted_at"]);
    return (
        NormalizedPost(
            source=str(d["source"]),
            source_post_id=str(d["source_post_id"]),
            account_id=str(d["account_id"]),
            account_handle=str(d["account_handle"]),
            url=str(d["url"]),
            text=str(d["text"]),
            quoted_text=d.get("quoted_text"),
            is_reply=bool(d.get("is_reply", False)),
            posted_at=posted,
            received_at=posted + timedelta(seconds=30),
            media_urls=list(d.get("media_urls", [])),
            tier=str(d.get("tier", "osint"))
        ),
        (d.get("expect", {}) as Json)
    );
}

def load_fixture(name: str) -> list[tuple[NormalizedPost, Json]] {
    data = json.loads((FIXTURES_DIR / f"{name}.json").read_text());
    return [fixture_post(p) for p in data["posts"]];
}

def all_fixture_posts -> list[tuple[str, NormalizedPost, Json]] {
    out: list[tuple[str, NormalizedPost, Json]] = [];
    for f in sorted(FIXTURES_DIR.glob("*.json")) {
        data = json.loads(f.read_text());
        for p in data["posts"] {
            (post, expect) = fixture_post(p);
            out.append((f"{data['event']}:{post.source_post_id}", post, expect));
        }
    }
    return out;
}
