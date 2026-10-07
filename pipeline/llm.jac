"""Anthropic Messages API wrapper.

Responsibilities (CLAUDE.md §7, §12):
- strict JSON responses with one "Return only JSON." retry
- per-call logging: model, input/output tokens, latency, post id
- consecutive-error counter driving pipeline fail-open (3+ errors)
- optional cassette record/replay (NEWSSTREAM_CASSETTE=path,
  NEWSSTREAM_CASSETTE_MODE=replay|record|auto) so tests and fixture replay run
  offline and deterministically.

Model quirk handling: current 4.6+/5-family models reject `temperature` and
some older models reject `output_config`; rather than hardcoding model-family
tables, a param rejected by the API with a 400 naming it is stripped and
remembered for that model.
"""

import hashlib;
import importlib;
import json;
import logging;
import os;
import time;
import from pathlib { Path }
import from adapters.base { Json }
import from db.repo { Repo }

glob log: logging.Logger = logging.getLogger("newsstream.llm");

# The Anthropic SDK is loaded at runtime instead of with `import anthropic;`.
# A static import makes the Jac compiler analyse the SDK's own dependencies, and
# jac 0.37.25 rejects the `typing.overload` methods in httpx2 (E0076), which
# stops the program from starting. Only this module touches the SDK.
glob anthropic: any = importlib.import_module("anthropic");

glob FAIL_OPEN_AFTER: int = 3;

"""The API errored; caller decides fail-open behavior."""
class LLMUnavailable(Exception) {}

"""The model answered but not with parseable JSON (after one retry)."""
class LLMParseError(Exception) {
    has raw: str = "";

    def init(self: LLMParseError, raw: str) {
        super.init(f"unparseable JSON: {raw[:200]!r}");
        self.raw = raw;
    }
}

def _extract_json(text: str) -> Json {
    s = text.strip();
    if s.startswith("```") {
        s = s.split("\n", 1)[1] if "\n" in s else s;
        s = s.rsplit("```", 1)[0];
    }
    (start, end) = (s.find("{"), s.rfind("}"));
    if start == -1 or end <= start {
        raise ValueError("no JSON object found");
    }
    return json.loads(s[start:end + 1]);
}

obj Cassette {
    has path: Path,
        mode: str;  # replay | record | auto

    has data: dict[str, Json] postinit;

    def postinit {
        self.data = {};
        if self.path.exists() {
            self.data = json.loads(self.path.read_text());
        }
    }

    static def key(model: str, system: str, user: str, max_tokens: int) -> str {
        blob = json.dumps(
            {"model": model, "system": system, "user": user, "max_tokens": max_tokens},
            sort_keys=True
        );
        return hashlib.sha256(blob.encode()).hexdigest()[:32];
    }

    def get(key: str) -> Json | None {
        return self.data.get(key);
    }

    def put(key: str, value: Json) {
        self.data[key] = value;
        self.path.parent.mkdir(parents=True, exist_ok=True);
        self.path.write_text(json.dumps(self.data, indent=1, sort_keys=True));
    }
}

def cassette_from_env -> Cassette | None {
    path = os.environ.get("NEWSSTREAM_CASSETTE");
    if not path {
        return None;
    }
    return Cassette(Path(path), os.environ.get("NEWSSTREAM_CASSETTE_MODE", "auto"));
}

class CassetteMiss(LLMUnavailable) {}

class _NoCredentials(Exception) {}

obj LLMClient {
    has api_key: str,
        repo: Repo | None = None,
        cassette: Cassette | None = None;

    has _client: any postinit;  # anthropic.AsyncAnthropic, or None without credentials
    has consecutive_errors: int postinit;
    has _rejected_params: dict[str, set[str]] postinit;
    has fail_open: bool {
        getter -> bool {
            return self.consecutive_errors >= FAIL_OPEN_AFTER;
        }
    }

    def postinit {
        try {
            self._client = anthropic.AsyncAnthropic(api_key=self.api_key or None);
        } except Exception as e {
            # no key, no auth profile: run in fail-open mode
            log.error("Anthropic client unavailable (%s); pipeline will fail open", e);
            self._client = None;
        }
        self.cassette = self.cassette or cassette_from_env();
        self.consecutive_errors = 0;
        self._rejected_params = {};
    }

    """One API call -> {"text", "input_tokens", "output_tokens"}, via cassette if configured."""
    async def _create_raw(
        model: str, system: str, user: str, max_tokens: int, prefer: Json
    ) -> Json {
        key = Cassette.key(model, system, user, max_tokens);
        if self.cassette {
            hit = self.cassette.get(key);
            if hit is not None and self.cassette.mode in ("replay", "auto") {
                return hit;
            }
            if self.cassette.mode == "replay" {
                raise CassetteMiss(
                    f"cassette miss for {model} in replay mode (key {key})"
                );
            }
        }

        if self._client is None {
            raise _NoCredentials("no Anthropic credentials configured");
        }
        rejected = self._rejected_params.setdefault(model, set());
        kwargs = {
            k: v
            for (k, v) in prefer.items()
            if k not in rejected
        };
        resp: any = None;
        while True {
            try {
                resp = await self._client.messages.create(
                    model=model,
                    system=system,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": user}],
                    **kwargs
                );
                break;
            } except (anthropic.BadRequestError, TypeError) as e {
                # Server 400 naming a param (model doesn't support it) or a
                # client-side "unexpected keyword argument" (SDK dropped it).
                # The SDK also raises TypeError at request time when it can't
                # resolve any credentials; that's an availability problem.
                if isinstance(e, TypeError) and "authentication" in str(e).lower() {
                    raise _NoCredentials(str(e)) from e;
                }
                culprit = next(
                    (
                        k
                        for k in list(kwargs)
                        if k in str(e)
                    ),
                    None
                );
                if culprit is None {
                    raise;
                }
                rejected.add(culprit);
                kwargs.pop(culprit);
                log.info(
                    "param %r not accepted for %s; retrying without it", culprit, model
                );
            }
        }

        out: Json = {
            "text": "".join(
                b.text
                for b in resp.content
                if b.type == "text"
            ),
            "input_tokens": resp.usage.input_tokens,
            "output_tokens": resp.usage.output_tokens
        };
        if self.cassette and self.cassette.mode in ("record", "auto") {
            self.cassette.put(key, out);
        }
        return out;
    }

    """JSON-returning call. Raises LLMUnavailable (API down) or LLMParseError."""
    async def call_json(
        purpose: str,
        model: str,
        system: str,
        user: str,
        *,
        max_tokens: int = 400,
        post_id: int | None = None,
        prefer: Json | None = None
    ) -> Json {
        prefer = prefer or {};
        attempts = [user, user + "\n\nReturn only JSON."];
        raw = "";
        for (attempt, msg) in enumerate(attempts) {
            t0 = time.monotonic();
            resp: Json = {};
            try {
                resp = await self._create_raw(model, system, msg, max_tokens, prefer);
            } except CassetteMiss {
                raise;
            } except (anthropic.APIError, _NoCredentials) as e {
                self.consecutive_errors += 1;
                log.warning(
                    "llm call failed | purpose=%s model=%s post=%s consecutive_errors=%d err=%s",
                    purpose,
                    model,
                    post_id,
                    self.consecutive_errors,
                    e
                );
                raise LLMUnavailable(str(e)) from e;
            }
            latency_ms = int((time.monotonic() - t0) * 1000);
            self.consecutive_errors = 0;
            log.info(
                "llm call | purpose=%s model=%s post=%s in=%d out=%d latency=%dms",
                purpose,
                model,
                post_id,
                resp["input_tokens"],
                resp["output_tokens"],
                latency_ms
            );
            log.debug("llm prompt | %s", msg);
            log.debug("llm response | %s", resp["text"]);
            if self.repo {
                await self.repo.insert_llm_call(
                    purpose,
                    model,
                    post_id,
                    int(resp["input_tokens"]),
                    int(resp["output_tokens"]),
                    latency_ms
                );
            }
            raw = str(resp["text"]);
            try {
                return _extract_json(raw);
            } except (ValueError, json.JSONDecodeError) {
                if attempt == 0 {
                    log.warning(
                        "JSON parse failure, retrying once | purpose=%s post=%s",
                        purpose,
                        post_id
                    );
                    continue;
                }
            }
        }
        raise LLMParseError(raw);
    }
}
