"""Classifier tests (CLAUDE.md §11): offline logic + cassette-backed fixture scoring."""

import asyncio;
import from jaclang.testing.test { parametrize }
import from adapters.base { Json, NormalizedPost }
import from pipeline.classify { _parse, classify, compute_score }
import from pipeline.llm { LLMParseError, LLMUnavailable }
import from pipeline.notify { Bus }
import from pipeline.runner { Pipeline }
import from tests.helpers {
    all_fixture_posts,
    classify_response,
    load_fixture,
    make_settings,
    open_repo,
    real_llm,
    requires_llm,
    stub_llm,
    sub_scores
}

glob WEIGHTS: dict[str, float] = {
         "event_not_commentary": 3.0,
         "flow_impact": 3.0,
         "primary_source": 1.5,
         "specificity": 1.5,
         "novelty_prior": 1.0
     };

# ---------------- offline: scoring formula and parsing ----------------

test "score formula bounds" {
    assert compute_score(sub_scores(10, 10, 10, 10, 10), WEIGHTS) == 100;
    assert compute_score(sub_scores(0, 0, 0, 0, 0), WEIGHTS) == 0;
}

test "score formula weighted" {
    # (8*3 + 7*3 + 5*1.5 + 6*1.5 + 4*1) / 10 * 10 = 65.5 -> 66
    assert compute_score(sub_scores(8, 7, 5, 6, 4), WEIGHTS) == 66;
}

test "parse clamps and category fallback" {
    raw = classify_response(
        {"event_not_commentary": 99, "flow_impact": -3}, category="NOT_A_CATEGORY"
    );
    cls = _parse(raw, "triage", "m", WEIGHTS);
    assert cls.sub_scores["event_not_commentary"] == 10;
    assert cls.sub_scores["flow_impact"] == 0;
    assert cls.sub_scores["specificity"] == 0;  # missing -> 0
    assert cls.category == "OTHER";
}

# ---------------- offline: model routing ----------------

test "high score skips review" {
    async def body {
        settings = make_settings();
        (post, _) = load_fixture("kharg_strikes")[0];
        # 95, outside band
        llm = stub_llm([classify_response(sub_scores(10, 10, 10, 10, 5))]);
        results = await classify(llm, settings, post, 1);
        assert len(results) == 1 and results[0].stage == "triage";
    }
    asyncio.run(body());
}

test "band score reruns on review model" {
    async def body {
        settings = make_settings();
        (post, _) = load_fixture("kharg_strikes")[0];
        llm = stub_llm(
            [
                classify_response(sub_scores(7, 8, 7, 7, 7)),  # 73: within 70 +/- 8
                classify_response(sub_scores(9, 9, 8, 8, 7))  # review verdict
            ]
        );
        results = await classify(llm, settings, post, 1);
        assert [r.stage for r in results] == ["triage", "review"];
        assert llm.calls[0]["model"] == settings.triage_model;
        assert llm.calls[1]["model"] == settings.review_model;
        # (9*3 + 9*3 + 8*1.5 + 8*1.5 + 7*1) / 10 * 10 = 85; review result is final
        assert results[-1].materiality_score == 85;
    }
    asyncio.run(body());
}

test "parse failure fails open at threshold" {
    async def body {
        settings = make_settings();
        (post, _) = load_fixture("kharg_strikes")[0];
        llm = stub_llm([LLMParseError("not json")]);
        results = await classify(llm, settings, post, 1);
        final = results[-1];
        assert final.error == "classification_error";
        # surfaces at threshold
        assert final.materiality_score == settings.market.threshold;
    }
    asyncio.run(body());
}

# ---------------- offline: API-down fail-open through the pipeline ----------------

test "three errors switch to unfiltered for official tier" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            pipeline = Pipeline(
                settings, repo, stub_llm([LLMUnavailable("boom")] * 4), Bus()
            );
            posts = load_fixture("khasab_vlcc");
            official = next(
                p
                for (p, _) in posts
                if p.tier == "official"
            );
            tracker = next(
                p
                for (p, _) in posts
                if p.tier == "tracker"
            );
            osint = next(
                p
                for (p, _) in posts
                if p.tier == "osint"
            );
            assert await pipeline.process(tracker) == "held-llm-down";  # error 1
            assert await pipeline.process(osint) == "held-llm-down";  # error 2
            # error 3 -> fail open
            assert await pipeline.process(official) == "UNFILTERED";
            # osint stays held even in fail-open mode
            osint2 = load_fixture("noise")[0][0];
            assert await pipeline.process(osint2) == "held-llm-down";
        }
    }
    asyncio.run(body());
}

test "duplicate delivery skipped" {
    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            llm = stub_llm([classify_response(sub_scores(2, 2, 2, 2, 2))] * 2);
            pipeline = Pipeline(settings, repo, llm, Bus());
            (post, _) = load_fixture("noise")[0];
            assert await pipeline.process(post) == "suppressed";
            assert await pipeline.process(post) == "seen";
            assert len(llm.calls) == 1;  # no second classification
        }
    }
    asyncio.run(body());
}

# ---------------- cassette/live: fixture posts score on the right side ----------------

# (test id, post, expectations)
type FixtureCase = tuple[str, NormalizedPost, Json];

def _test_fixture_classification(item: FixtureCase) {
    requires_llm();
    (_, post, expect) = item;

    async def body {
        settings = make_settings();
        async with open_repo(settings) as repo {
            results = await classify(real_llm(repo), settings, post, None);
            final = results[-1];
            assert final.error is None , f"classifier error: {final.error}";
            threshold = settings.market.threshold;
            if expect["surfaced"] {
                assert final.materiality_score >= threshold , f"expected surfaced, got {final.materiality_score} ({final.sub_scores})";
            } else {
                cap = expect.get("max_score", threshold - 1);
                assert final.materiality_score <= cap , f"expected <= {cap}, got {final.materiality_score} ({final.sub_scores})";
            }
            assert final.category in expect["category_in"] , f"category {final.category} not in {expect[
                'category_in'
            ]}";
        }
    }
    asyncio.run(body());
}

with entry {
    parametrize(
        "fixture classification",
        all_fixture_posts(),
        _test_fixture_classification,
        id_fn=lambda (item: FixtureCase) -> str { return item[0]; }
    );
}
