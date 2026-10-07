"""FastAPI service: SSE stream, UI, status, ingest. `jac run` starts it."""

import asyncio;
import contextlib;
import json;
import logging;
import os;
import subprocess;
import uvicorn;
import from datetime { UTC, datetime }
import from pathlib { Path }
import from fastapi { FastAPI, HTTPException, Request }
import from fastapi.responses {
    FileResponse,
    HTMLResponse,
    Response,
    StreamingResponse
}
import from adapters.base { Json, NormalizedPost, SourceAdapter }
import from adapters.telegram { TelegramAdapter }
import from adapters.x_api { XApiAdapter }
import from adapters.x_push { XPushAdapter }
import from config { ROOT, Settings, load_settings }
import from db.repo { Repo, Row }
import from pipeline.llm { LLMClient }
import from pipeline.notify { Bus }
import from pipeline.runner { Pipeline }

glob log: logging.Logger = logging.getLogger("newsstream.app");

# The browser UI is Jac too (web/main.jac). `jac build web` compiles it into
# one self-contained page here; ensure_ui() runs that build when needed.
glob UI_SOURCE: Path = ROOT / "web",
     UI_INDEX: Path = ROOT / ".jac" / "client" / "web" / "dist" / "index.html";

glob UI_MISSING_HTML: str = (
         "<!doctype html><meta charset=utf-8><title>newsstream</title>"
         "<body style='font:14px sans-serif;background:#0e1116;color:#d7dde8;padding:24px'>"
         "<h1>newsstream is running, but its web page has not been built</h1>"
         "<p>Run <code>jac build web</code> in the project folder, then reload. "
         "The server log says why the automatic build failed.</p>"
     );

"""True when the built page is missing or older than its inputs: any file
under web/, or jac.toml (page title, npm dependencies)."""
def ui_stale -> bool {
    if not UI_INDEX.exists() {
        return True;
    }
    built = UI_INDEX.stat().st_mtime;
    inputs = [
        p
        for p in UI_SOURCE.rglob("*")
        if p.is_file()
    ]
        + [ROOT / "jac.toml"];
    return `any(p.stat().st_mtime > built for p in inputs);
}

"""Build the browser UI with `jac build web` if it is missing or stale.
Never raises: without a page the service still ingests and classifies."""
def ensure_ui {
    if not ui_stale() {
        return;
    }
    jac = os.environ.get("JAC_EXECUTABLE") or "jac";
    log.info("building the web UI (%s build web) ...", jac);
    try {
        done = subprocess.run(
            [jac, "build", "web"], cwd=ROOT, capture_output=True, text=True, timeout=600
        );
        if done.returncode != 0 or not UI_INDEX.exists() {
            log.error(
                "web UI build failed (exit %d); run `jac build web` to see why\n%s",
                done.returncode,
                (done.stdout + done.stderr)[-2000:]
            );
        } else {
            log.info("web UI built: %s", UI_INDEX);
        }
    } except (OSError, subprocess.SubprocessError) as e {
        log.error("web UI build could not run (%s); run `jac build web` by hand", e);
    }
}

async def _guarded(adapter: SourceAdapter, work: Awaitable[None]) {
    try {
        await work;
    } except asyncio.CancelledError {
        raise;
    } except Exception as e {
        adapter.status.state = "error";
        adapter.status.detail = str(e)[:160];
        log.exception("adapter %s crashed", adapter.status.name);
    }
}

"""Resolve configured handles to stable IDs once and store both (§4)."""
async def _resolve_x_accounts(settings: Settings, repo: Repo, x_api: XApiAdapter) {
    known_handles = {str(a["handle"]).lower() for a in await repo.get_accounts("x")};
    for acct in settings.accounts {
        if acct.source != "x" or acct.handle.lower() in known_handles {
            continue;
        }
        try {
            (account_id, canonical) = await x_api.resolve_account(acct.handle);
            await repo.upsert_account("x", account_id, canonical, acct.tier);
            log.info("resolved @%s -> id %s", canonical, account_id);
        } except Exception as e {
            log.error(
                "could not resolve X handle @%s: %s (renamed? update config.yaml)",
                acct.handle,
                e
            );
        }
    }
}

def create_app(settings: Settings | None = None) -> FastAPI {
    settings = settings or load_settings();
    repo = Repo(settings.db_path);
    bus = Bus();
    llm = LLMClient(settings.anthropic_api_key, repo);
    pipeline = Pipeline(settings, repo, llm, bus);

    telegram = TelegramAdapter(settings);
    x_api = XApiAdapter(settings, repo);
    x_push = XPushAdapter(settings, repo);
    adapters: list[SourceAdapter] = [telegram, x_api, x_push];

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None] {
        await repo.connect();
        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(pipeline.run(), name="pipeline"),
            asyncio.create_task(pipeline.stale_sweeper(), name="stale-sweeper")
        ];
        if settings.x_api_enabled and settings.x_bearer_token {
            await _resolve_x_accounts(settings, repo, x_api);
        }
        for adapter in adapters {
            tasks.append(
                asyncio.create_task(
                    _guarded(adapter, adapter.run(pipeline.sink)),
                    name=adapter.status.name
                )
            );
        }
        log.info("newsstream up at http://%s:%d", settings.host, settings.port);
        try {
            yield ;
        } finally {
            for t in tasks {
                t.cancel();
            }
            await asyncio.gather(*tasks, return_exceptions=True);
            await repo.close();
        }
    }

    app = FastAPI(title="newsstream", lifespan=lifespan);
    setattr(app.state, "pipeline", pipeline);
    setattr(app.state, "repo", repo);
    setattr(app.state, "settings", settings);

    @app.get("/")
    async def index -> Response {
        if not UI_INDEX.exists() {
            return HTMLResponse(UI_MISSING_HTML, status_code=503);
        }
        # no-cache: the page is rebuilt in place, and it is one small local file
        return FileResponse(UI_INDEX, headers={"Cache-Control": "no-cache"});
    }

    @app.get("/stream")
    async def stream(request: Request) -> StreamingResponse {
        async def gen -> AsyncIterator[str] {
            async with bus.subscribe() as q {
                yield ": connected\n\n";
                while not await request.is_disconnected() {
                    try {
                        payload = (await asyncio.wait_for(q.get(), timeout=15)) as Json;
                        yield (
                            f"id: {payload['notification_id']}\n"
                            f"event: notification\ndata: {json.dumps(payload)}\n\n"
                        );
                    } except TimeoutError {
                        yield ": heartbeat\n\n";
                    }
                }
            }
        }
        return StreamingResponse(
            gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        );
    }

    @app.get("/notifications")
    async def notifications(since: int = 0) -> list[Row] {
        return await repo.notifications_since(since);
    }

    @app.get("/feed")
    async def feed -> Json {
        return {"events": await repo.feed()};
    }

    @app.get("/suppressed")
    async def suppressed -> Json {
        return {"posts": await repo.suppressed()};
    }

    @app.get("/status")
    async def status -> Json {
        usage = await repo.llm_usage_today();
        spend = sum(
            r["input_tokens"] / 1e6 * settings.price_for(r["model"])[0]
                + r["output_tokens"] / 1e6 * settings.price_for(r["model"])[1]
            for r in usage
        );
        inactive = [
            a["handle"]
            for a in await repo.get_accounts()
            if a["status"] == "inactive"
        ];
        return {
            "adapters": [vars(a.status) for a in adapters],
            "latency": pipeline.stats.snapshot(),
            "counts": await repo.counts_today(),
            "llm_spend_today": round(spend, 2),
            "llm_fail_open": llm.fail_open,
            "x_reads_today": await repo.x_reads_today(),
            "refetch_running": x_push.refetch_running,
            "x_daily_read_budget": settings.x_daily_read_budget,
            "threshold": settings.market.threshold,
            "max_stacked": settings.ui_max_stacked,
            "inactive_accounts": inactive
        };
    }

    """Re-pull every account's recent posts (UI catch-up button)."""
    @app.post("/refetch")
    async def refetch(hours: float = 12) -> Json {
        hours = max(1.0, min(48.0, hours));
        if not settings.x_push_api_key {
            raise HTTPException(409, "x_push is not configured");
        }
        if x_push.refetch_running {
            raise HTTPException(409, "refetch already running");
        }
        asyncio.create_task(x_push.refetch(pipeline.sink, hours));
        return {"started": True, "hours": hours};
    }

    """Local ingestion endpoint used by `jac run replay.jac`."""
    @app.post("/ingest")
    async def ingest(body: Json) -> Json {
        post: NormalizedPost;
        try {
            post = NormalizedPost(
                source=body["source"],
                source_post_id=str(body["source_post_id"]),
                account_id=str(body.get("account_id", body.get("account_handle", ""))),
                account_handle=body["account_handle"],
                url=body.get("url", ""),
                text=body.get("text", ""),
                quoted_text=body.get("quoted_text"),
                is_reply=bool(body.get("is_reply", False)),
                posted_at=datetime.fromisoformat(body["posted_at"]),
                received_at=(
                    datetime.fromisoformat(body["received_at"])
                        if body.get("received_at")
                        else datetime.now(UTC)
                ),
                media_urls=list(body.get("media_urls", [])),
                tier=body.get("tier", "osint")
            );
        } except (KeyError, ValueError) as e {
            raise HTTPException(422, f"bad post: {e}") from e;
        }
        await pipeline.sink(post);
        return {"queued": True};
    }

    return app;
}

def main {
    level = logging.DEBUG if os.environ.get("NEWSSTREAM_DEBUG") else logging.INFO;
    # force: the jac runtime installs its own root handler before this runs,
    # and basicConfig is otherwise a no-op once any handler exists.
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        force=True
    );
    logging.getLogger("httpx").setLevel(logging.WARNING);
    settings = load_settings();
    ensure_ui();
    # timeout_graceful_shutdown: open SSE streams never finish on their own;
    # without this cap a SIGTERM'd server waits on them forever and zombies
    # (holding the DB and the vendor's one-WS-per-key slot; seen 2026-09-11).
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="warning",
        timeout_graceful_shutdown=5
    );
}

with entry:__main__ {
    main();
}
