"""Dedup tests (CLAUDE.md §11): offline clustering logic + cassette-backed
sequence replays producing the expected NEW_EVENT / UPDATE / DUPLICATE labels."""

import asyncio;
import from datetime { timedelta }
import from jaclang.testing.test { parametrize }
import from db.repo { Row, utcnow }
import from pipeline.classify { Classification }
import from pipeline.dedup {
    decide,
    entities_overlap,
    fetch_open_events,
    filter_candidates,
    merged_entities
}
import from pipeline.notify { Bus }
import from pipeline.runner { Pipeline }
import from tests.helpers {
    load_fixture,
    make_settings,
    open_repo,
    real_llm,
    requires_llm,
    stub_llm
}

def _cls(
    category: str = "STRIKE_MILITARY",
    vessels: list[str] | None = None,
    locations: list[str] | None = None,
    actors: list[str] | None = None
) -> Classification {
    return Classification(
        stage="triage",
        model="stub",
        materiality_score=80,
        category=category,
        one_line="x",
        entities={
            "vessels": list(vessels or []),
            "locations": list(locations or []),
            "actors": list(actors or [])
        }
    );
}

def _event(
    eid: int,
    category: str = "STRIKE_MILITARY",
    vessels: list[str] | None = None,
    locations: list[str] | None = None
) -> Row {
    return {
        "id": eid,
        "category": category,
        "title": f"ev{eid}",
        "summary": "s",
        "entities": {
            "vessels": list(vessels or []),
            "locations": list(locations or []),
            "actors": []
        }
    };
}

# ---------------- offline: candidate selection ----------------

test "candidates by category or entity overlap" {
    events = [
        _event(1, category="SHIPPING_INCIDENT", vessels=["M/T Downy"]),
        _event(2, category="STRIKE_MILITARY"),  # category match
        _event(3, category="DIPLOMACY", locations=["Kharg Island"]),  # entity match
        _event(4, category="DIPLOMACY", locations=["Vienna"])  # no match
    ];
    cls = _cls(
        category="STRIKE_MILITARY", vessels=["m/t downy"], locations=["KHARG ISLAND"]
    );
    picked = [e["id"] for e in filter_candidates(events, cls)];
    assert picked == [1, 2, 3];
}

test "candidates capped at eight" {
    events = [_event(i) for i in range(20)];
    assert len(filter_candidates(events, _cls())) == 8;
}

test "entity overlap is fuzzy" {
    # "Jazan" must match an event holding "Jazan refinery" (substring/token),
    # or a confirmation post never reaches the dedup LLM at all.
    events = [_event(1, category="STRIKE_MILITARY", locations=["Jazan refinery"])];
    cls = _cls(category="OFFICIAL_STATEMENT", locations=["Jazan"]);
    assert [e["id"] for e in filter_candidates(events, cls)] == [1];
    # shared token
    assert entities_overlap({"saudi aramco"}, {"saudi energy ministry"});
    # no short-substring false positive
    assert not entities_overlap({"us"}, {"houthis"});
}

test "no candidates is new event without llm" {
    async def body {
        settings = make_settings();
        llm = stub_llm();
        (post, _) = load_fixture("kharg_strikes")[0];
        result = await decide(llm, settings, post, _cls(), [], None);
        assert result.decision == "NEW_EVENT";
        assert llm.calls == [];
    }
    asyncio.run(body());
}

test "bad event id clamped to newest candidate" {
    async def body {
        settings = make_settings();
        llm = stub_llm([{"decision": "DUPLICATE", "event_id": 999, "reasoning": "r"}]);
        (post, _) = load_fixture("kharg_strikes")[0];
        result = await decide(
            llm, settings, post, _cls(), [_event(5), _event(6)], None
        );
        assert result.decision == "DUPLICATE" and result.event_id == 5;
    }
    asyncio.run(body());
}

test "merged entities case insensitive union" {
    merged = merged_entities(
        {"vessels": ["M/T Downy"], "locations": ["Kharg"], "actors": []},
        {"vessels": ["m/t downy", "Stark 1"], "locations": [], "actors": ["CENTCOM"]}
    );
    assert merged["vessels"] == ["M/T Downy", "Stark 1"];
    assert merged["actors"] == ["CENTCOM"];
}

# ---------------- offline: staleness ----------------

test "stale events leave candidate pool" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            eid = await repo.create_event("STRIKE_MILITARY", "old strike", "s", {});
            await repo.db.execute(
                "UPDATE events SET last_updated_at = ? WHERE id = ?",
                ((utcnow() - timedelta(hours=30)).isoformat(), eid)
            );
            await repo.db.commit();
            assert await fetch_open_events(repo, settings) == [];
            assert await repo.mark_stale_events(settings.stale_after_hours) == 1;
            event = await repo.get_event(eid);
            assert event is not None and event["status"] == "stale";
        }
    }
    asyncio.run(body());
}

test "feed sorted by origin post recency" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            older = load_fixture("khasab_vlcc")[0][0];
            newer = load_fixture("kharg_strikes")[0][0];
            id_old = await repo.insert_post(older);  # posted Aug 31
            id_new = await repo.insert_post(newer);  # posted Sep 5
            assert id_old is not None and id_new is not None;
            ev_old = await repo.create_event("SHIPPING_INCIDENT", "old story", "s", {});
            ev_new = await repo.create_event("STRIKE_MILITARY", "new story", "s", {});
            await repo.link_event_post(ev_old, id_old, "origin");
            await repo.link_event_post(ev_new, id_new, "origin");
            # Update the OLD story so last_updated_at would sort it first...
            await repo.update_event(ev_old, "updated summary");
            feed = await repo.feed();
            # ...but the feed must order by origin post recency instead.
            assert [e["title"] for e in feed] == ["new story", "old story"];
            # A stale event sinks below ALL open events, even with the newest origin.
            await repo.db.execute(
                "UPDATE events SET status = 'stale' WHERE id = ?", (ev_new, )
            );
            await repo.db.commit();
            feed = await repo.feed();
            assert [e["title"] for e in feed] == ["old story", "new story"];
        }
    }
    asyncio.run(body());
}

# ---------------- cassette/live: fixture sequences replay in order ----------------

def _test_sequence_labels(name: str) {
    requires_llm();

    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            pipeline = Pipeline(settings, repo, real_llm(repo), Bus());
            outcomes: list[str] = [];
            expected: list[str] = [];
            for (post, expect) in load_fixture(name) {
                outcomes.append(await pipeline.process(post));
                expected.append(
                    str(expect["dedup"]) if expect["surfaced"] else "suppressed"
                );
            }
            assert outcomes == expected , f"{outcomes} != {expected}";
        }
    }
    asyncio.run(body());
}

with entry {
    parametrize(
        "sequence labels",
        ["khasab_vlcc", "kharg_strikes", "jazan_refinery", "irgc_warning"],
        _test_sequence_labels,
        id_fn=lambda (name: str) -> str { return name; }
    );
}
