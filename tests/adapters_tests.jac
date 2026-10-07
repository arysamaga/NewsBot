"""Adapter tests (CLAUDE.md §11): replies dropped, quote text carried,
handle change on stable ID handled, unresolvable ID marks account inactive."""

import asyncio;
import httpx;
import from datetime { UTC, datetime, timedelta }
import from types { SimpleNamespace }
import from adapters.base { Json, NormalizedPost }
import from adapters.telegram { bare_chat_id, bot_update_to_post, message_to_post }
import from adapters.x_api { XApiAdapter, parse_timeline }
import from adapters.x_push {
    RULE_VALUE_MAX,
    XPushAdapter,
    build_rule_values,
    parse_ws_message
}
import from pipeline.normalize { normalize }
import from tests.helpers { load_fixture, make_settings, open_repo }

glob NOW: datetime = datetime(2026, 9, 10, 12, 0, tzinfo=UTC);

# ---------------- X timeline parsing ----------------

glob TIMELINE: Json = {
         "data": [
             {
                 "id": "300",
                 "text": "newest plain post",
                 "created_at": "2026-09-10T11:59:00.000Z"
             },
             {
                 "id": "200",
                 "text": "confirmed.",
                 "created_at": "2026-09-10T11:50:00.000Z",
                 "referenced_tweets": [{"type": "quoted", "id": "150"}]
             },
             {
                 "id": "100",
                 "text": "@someone i agree",
                 "created_at": "2026-09-10T11:40:00.000Z",
                 "referenced_tweets": [{"type": "replied_to", "id": "90"}]
             }
         ],
         "includes": {
             "tweets": [
                 {"id": "150", "text": "original report: tanker struck off Kharg"}
             ],
             "users": [{"id": "89234502", "username": "TankerTrackers_new"}]
         }
     };

test "replies dropped and quotes carried" {
    (posts, _) = parse_timeline(
        TIMELINE, "89234502", "TankerTrackers", received_at=NOW
    );
    # reply gone, oldest first
    assert [p.source_post_id for p in posts] == ["200", "300"];
    quoted = posts[0];
    assert quoted.quoted_text == "original report: tanker struck off Kharg";
    assert posts[1].quoted_text is None;
    assert all(not p.is_reply for p in posts);
}

test "handle refreshed from stable id" {
    (posts, handle) = parse_timeline(
        TIMELINE, "89234502", "TankerTrackers", received_at=NOW
    );
    assert handle == "TankerTrackers_new";  # rename picked up from the ID
    assert all(p.account_id == "89234502" for p in posts);
    assert "TankerTrackers_new" in posts[0].url;
}

test "handle change same id is one account" {
    async def body {
        async with open_repo(make_settings()) as repo {
            await repo.upsert_account("x", "89234502", "TankerTrackers", "tracker");
            await repo.upsert_account("x", "89234502", "TankerTrackers_new", "tracker");
            accounts = await repo.get_accounts("x");
            assert len(accounts) == 1;
            assert accounts[0]["handle"] == "TankerTrackers_new";
        }
    }
    asyncio.run(body());
}

test "unresolvable id marks account inactive" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            await repo.upsert_account("x", "89234502", "TankerTrackers", "tracker");
            adapter = XApiAdapter(settings, repo);
            req = httpx.Request("GET", "https://api.x.com/2/users/89234502/tweets");
            err = httpx.HTTPStatusError(
                "gone", request=req, response=httpx.Response(404, request=req)
            );
            acct = (await repo.get_accounts("x"))[0];
            await adapter._handle_http_error(acct, err);
            assert (await repo.get_accounts("x"))[0]["status"] == "inactive";
            assert "inactive" in adapter.status.detail;
        }
    }
    asyncio.run(body());
}

# ---------------- Telegram converters ----------------

def _tg_msg(
    text: str = "mines reported near Hormuz TSS", reply_to: object | None = None
) -> SimpleNamespace {
    return SimpleNamespace(
        id=42,
        message=text,
        reply_to=reply_to,
        date=datetime(2026, 9, 10, 11, 58, tzinfo=UTC)
    );
}

test "telegram message converted" {
    post = message_to_post(_tg_msg(), 12345, "faytuks", received_at=NOW);
    assert post is not None;
    assert post.source == "telegram";
    assert post.source_post_id == "12345:42";
    assert post.account_id == "12345";
    assert post.url == "https://t.me/faytuks/42";
    assert post.posted_at.tzinfo is not None;
}

test "telegram reply dropped" {
    assert message_to_post(_tg_msg(reply_to=object()), 12345, "faytuks") is None;
}

test "bot update converted and reply dropped" {
    update: Json = {
        "update_id": 1,
        "channel_post": {
            "message_id": 7,
            "date": 1789500000,
            "text": "UKMTO advisory 032-26",
            "chat": {"id": -1001234, "username": "ukmto_mirror"}
        }
    };
    post = bot_update_to_post(update, received_at=NOW);
    assert post is not None;
    assert post.source_post_id == "-1001234:7";
    assert post.posted_at == datetime.fromtimestamp(1789500000, UTC);
    update["channel_post"]["reply_to_message"] = {"message_id": 3};
    assert bot_update_to_post(update) is None;
}

test "bare chat id" {
    assert bare_chat_id(-1001234567) == 1234567;
    assert bare_chat_id(555) == 555;
}

# ---------------- X push (TwitterAPI.io) parsing ----------------

glob ALLOWED: set[str] = {"uk_mto", "tankertrackers", "faytuksnetwork"};

test "x push rule matched event parsed" {
    msg: Json = {
        "event_type": "tweet",
        "rule_id": "r1",
        "rule_tag": "newsstream-0",
        "tweets": [
            {
                "id": "555",
                "text": "INCIDENT 033-26: vessel reports being hailed",
                "author": {"id": "717836935", "userName": "UK_MTO", "name": "UKMTO"},
                "createdAt": "Thu Sep 10 11:22:33 +0000 2026",
                "quoted_tweet": {"id": "500", "text": "original advisory"}
            },
            {
                "id": "556",
                "text": "reply text",
                "isReply": True,
                "author": {"id": "1", "userName": "UK_MTO"},
                "createdAt": "Thu Sep 10 11:23:00 +0000 2026"
            },
            {
                "id": "557",
                "text": "RT @someone: original text",
                "retweeted_tweet": {"id": "500"},
                "author": {"id": "1", "userName": "UK_MTO"},
                "createdAt": "Thu Sep 10 11:24:00 +0000 2026"
            }
        ]
    };
    posts = parse_ws_message(msg, ALLOWED, received_at=NOW);
    assert len(posts) == 1;  # reply and pure retweet dropped
    p = posts[0];
    assert (p.account_id, p.account_handle) == ("717836935", "UK_MTO");
    assert p.quoted_text == "original advisory";
    assert p.posted_at == datetime(2026, 9, 10, 11, 22, 33, tzinfo=UTC);
    assert p.url == "https://x.com/UK_MTO/status/555";
}

test "x push fast tweet lane and handle filter" {
    fast: Json = {
        "event_type": "fast_tweet",
        "timestamp": 1789500000000,
        "tweet": {
            "id": "777",
            "screen_name": "TankerTrackers",
            "user_id": "89234502",
            "text": "vessel struck",
            "type": "post",
            "created_ms": 1789500000000,
            "media": ["https://pbs.example/img.jpg"]
        }
    };
    posts = parse_ws_message(fast, ALLOWED, received_at=NOW);
    assert len(posts) == 1;
    assert posts[0].account_id == "89234502";
    assert posts[0].media_urls == ["https://pbs.example/img.jpg"];
    assert posts[0].posted_at == datetime.fromtimestamp(1789500000, UTC);

    # fast_tweet events are not rule-attributed: unknown authors are filtered
    fast["tweet"]["screen_name"] = "SomeRandoAccount";
    assert parse_ws_message(fast, ALLOWED, received_at=NOW) == [];
    # ...and reply-typed fast tweets are dropped
    fast["tweet"].update(screen_name="TankerTrackers", type="reply");
    assert parse_ws_message(fast, ALLOWED, received_at=NOW) == [];
}

"""A fake vendor REST client that records calls and reports `existing_rules`."""
def _rules_transport(
    calls: list[tuple[str, str]], existing_rules: list[Json]
) -> httpx.AsyncClient {
    def handler(request: httpx.Request) -> httpx.Response {
        calls.append((request.method, request.url.path));
        if request.url.path == "/oapi/tweet_filter/get_rules" {
            return httpx.Response(
                200, json={"status": "success", "rules": existing_rules}
            );
        }
        return httpx.Response(
            200, json={"status": "success", "msg": "ok", "rule_id": "r1"}
        );
    }
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.twitterapi.io"
    );
}

test "sync never updates an active up to date rule" {
    # Regression: the vendor rejects update_rule on active rules; calling it
    # unconditionally crash-looped every reconnect (live incident 2026-09-10).
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            handles = [
                a.handle
                for a in settings.accounts
                if a.source == "x"
            ];
            value = build_rule_values(handles)[0];
            calls: list[tuple[str, str]] = [];
            existing: list[Json] = [
                {
                    "rule_id": "r1",
                    "tag": "newsstream-0",
                    "value": value,
                    "interval_seconds": settings.x_push_interval,
                    "is_effect": 1
                }
            ];
            rest = _rules_transport(calls, existing);
            async with rest {
                await XPushAdapter(settings, repo)._sync_rules(rest, handles);
            }
            assert calls == [("GET", "/oapi/tweet_filter/get_rules")];
        }
    }
    asyncio.run(body());
}

test "sync deactivates active rule before changing it" {
    # Changing the account list changes the rule value; an active rule must be
    # deactivated (accepted by the vendor) before the update is applied.
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            handles = [
                a.handle
                for a in settings.accounts
                if a.source == "x"
            ];
            calls: list[tuple[str, str]] = [];
            existing: list[Json] = [
                {
                    "rule_id": "r1",
                    "tag": "newsstream-0",
                    "value": "from:OldAccount",
                    "interval_seconds": settings.x_push_interval,
                    "is_effect": 1
                }
            ];
            rest = _rules_transport(calls, existing);
            async with rest {
                await XPushAdapter(settings, repo)._sync_rules(rest, handles);
            }
            assert calls
                == [
                    ("GET", "/oapi/tweet_filter/get_rules"),
                    ("POST", "/oapi/tweet_filter/update_rule"),  # is_effect=0, old value
                    ("POST", "/oapi/tweet_filter/update_rule")  # new value, is_effect=1
                ];
        }
    }
    asyncio.run(body());
}

test "sync activates inactive rule" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            handles = [
                a.handle
                for a in settings.accounts
                if a.source == "x"
            ];
            value = build_rule_values(handles)[0];
            calls: list[tuple[str, str]] = [];
            existing: list[Json] = [
                {
                    "rule_id": "r1",
                    "tag": "newsstream-0",
                    "value": value,
                    "interval_seconds": settings.x_push_interval,
                    "is_effect": 0
                }
            ];
            rest = _rules_transport(calls, existing);
            async with rest {
                await XPushAdapter(settings, repo)._sync_rules(rest, handles);
            }
            assert ("POST", "/oapi/tweet_filter/update_rule") in calls;
        }
    }
    asyncio.run(body());
}

test "poll once queries search and advances since" {
    # REST fallback when the vendor rejects the WebSocket (403, 2026-09-11).
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            seen_queries: list[dict[str, str]] = [];
            def handler(request: httpx.Request) -> httpx.Response {
                seen_queries.append(dict(request.url.params));
                return httpx.Response(
                    200,
                    json={
                        "has_next_page": False,
                        "next_cursor": "",
                        "tweets": [
                            {
                                "id": "901",
                                "text": "tanker update",
                                "createdAt": "Thu Sep 11 06:00:00 +0000 2026",
                                "author": {"id": "717836935", "userName": "UK_MTO"}
                            }
                        ]
                    }
                );
            }
            adapter = XPushAdapter(settings, repo);
            rest = httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                base_url="https://api.twitterapi.io"
            );
            async with rest {
                (posts, new_since) = await adapter._poll_once(
                    rest, ["from:UK_MTO"], 1000, {"uk_mto"}
                );
            }
            assert seen_queries[0]["query"] == "(from:UK_MTO) since_time:1000";
            assert seen_queries[0]["queryType"] == "Latest";
            assert [p.source_post_id for p in posts] == ["901"];
            assert new_since > 1000;  # advances to cycle start minus overlap
        }
    }
    asyncio.run(body());
}

test "backfill pages until cutoff" {
    # The refetch button pages each timeline back N hours; paging must stop
    # once a page reaches posts older than the cutoff.
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            now = datetime.now(UTC);
            fmt = "%a %b %d %H:%M:%S +0000 %Y";
            def tw(tid: str, age_h: int) -> Json {
                return {
                    "id": tid,
                    "text": f"post {tid}",
                    "createdAt": (now - timedelta(hours=age_h)).strftime(fmt),
                    "author": {"id": "717836935", "userName": "UK_MTO"}
                };
            }
            pages: dict[str, Json] = {
                "": {
                    "tweets": [tw("300", 1), tw("299", 5)],
                    "has_next_page": True,
                    "next_cursor": "c2"
                },
                "c2": {
                    "tweets": [tw("298", 9), tw("297", 20)],
                    "has_next_page": True,
                    "next_cursor": "c3"
                },
                "c3": {
                    "tweets": [tw("296", 30)],
                    "has_next_page": True,
                    "next_cursor": "c4"
                }
            };
            calls: list[str] = [];
            def handler(request: httpx.Request) -> httpx.Response {
                cursor = request.url.params.get("cursor", "");
                calls.append(cursor);
                return httpx.Response(200, json=pages[cursor]);
            }
            got: list[str] = [];
            async def sink(p: NormalizedPost) {
                got.append(p.source_post_id);
            }
            adapter = XPushAdapter(settings, repo);
            rest = httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                base_url="https://api.twitterapi.io"
            );
            async with rest {
                pushed = await adapter._backfill(
                    rest, ["UK_MTO"], {"uk_mto"}, sink, hours=12, max_pages=6
                );
            }
            # the 20h and 30h posts are beyond the window
            assert got == ["300", "299", "298"];
            assert pushed == 3;
            # stopped paging at the page that crossed the cutoff
            assert calls == ["", "c2"];
        }
    }
    asyncio.run(body());
}

test "x push rule values chunked under limit" {
    handles = [f"account_number_{i:03d}" for i in range(30)];
    values = build_rule_values(handles);
    assert all(len(v) <= RULE_VALUE_MAX for v in values);
    assert len(values) > 1;  # 30 long handles cannot fit one rule
    joined = " OR ".join(values);
    for h in handles {
        assert f"from:{h}" in joined;
    }
}

# ---------------- normalization ----------------

test "normalize drops replies and resolves tier" {
    settings = make_settings();
    (post, _) = load_fixture("khasab_vlcc")[0];
    post.tier = "osint";  # pretend the adapter didn't know
    normalized = normalize(post, settings);
    assert normalized is not None;
    assert normalized.tier == "tracker";  # TankerTrackers per config.yaml

    (reply, _) = load_fixture("khasab_vlcc")[1];
    reply.is_reply = True;
    assert normalize(reply, settings) is None;
}

test "normalize strips zero width characters" {
    settings = make_settings();
    (post, _) = load_fixture("khasab_vlcc")[0];
    post.text = "​Tanker‍ struck﻿ ";
    normalized = normalize(post, settings);
    assert normalized is not None and normalized.text == "Tanker struck";
}
