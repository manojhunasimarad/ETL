"""Step 11 without a network: the provider layer, the review labelling, the cache and the checks.

A stand-in model answers from the hand labels of docs/samples, so these tests cost no quota.
Run the real thing with `python -m ecom_etl enrich-reviews --source sample` (docs/LLM.md).
"""
import io
import json
import re
import urllib.error

import pandas as pd
import pytest

from ecom_etl import enrich as E
from ecom_etl import llm as L
from ecom_etl.ask import LlmError
from ecom_etl.cli import main

SHORT = {v: k for k, v in E.SENTIMENTS.items()}


class LabelModel:
    """Answers each prompt from the hand labels; can drop or corrupt items to test the checks."""

    model = "stand-in"

    def __init__(self, drop_first=0, corrupt=None):
        df = E.load_reviews(E.SAMPLE_FILE).merge(pd.read_csv(E.SAMPLE_LABELS, dtype=str), on="review_id")
        self.labels = {E.normalise(t): (s, top, f) for t, s, top, f in
                       zip(df["text"], df["sentiment"], df["topics"], df["follow_up"])}
        self.calls, self.drop_first, self.corrupt = [], drop_first, corrupt or set()

    def generate_json(self, system, prompt):
        texts = [json.loads(m) for m in re.findall(r"^\d+\. (.*)$", prompt, flags=re.MULTILINE)]
        self.calls.append(len(texts))
        results = []
        for i, t in enumerate(texts, start=1):
            if self.drop_first and len(self.calls) == 1 and i <= self.drop_first:
                continue  # the model "forgets" items in its first answer
            s, top, f = self.labels[E.normalise(t)]
            if E.normalise(t) in self.corrupt:
                s = "happy"  # never valid, even on the retry
            results.append({"i": i, "s": SHORT.get(s, s), "t": top.split("|"), "f": f == "true"})
        return {"results": results}, {"totalTokenCount": 100}


def run(tmp_path, llm, **kw):
    return E.enrich_reviews(E.SAMPLE_FILE, llm, llm.model, tmp_path, labels_path=E.SAMPLE_LABELS, **kw)


def test_sample_is_labelled_deduplicated_and_scored(tmp_path):
    llm = LabelModel()
    r = run(tmp_path, llm, batch_size=10)
    assert (r.plan.rows, r.plan.with_text, r.plan.unique_texts) == (40, 37, 35)  # "Muito bom" == "muito bom!"
    assert llm.calls == [10, 10, 10, 5] and r.stats.requests == 4
    assert len(r.enriched) == 37 and r.quarantine.empty            # duplicates get their label back
    assert r.golden["sentiment"] == 1.0 and r.golden["topic_jaccard"] == 1.0 and r.golden["follow_up"] == 1.0
    assert r.golden["baseline_sentiment"] < 1.0                     # the free rule is beaten
    assert (tmp_path / "reviews_enriched.csv").exists() and (tmp_path / "run.json").exists()


def test_second_run_is_free_thanks_to_the_cache(tmp_path):
    run(tmp_path, LabelModel(), batch_size=40)
    again = LabelModel()
    r = run(tmp_path, again, batch_size=40)
    assert again.calls == [] and r.stats.cache_hits == 35 and len(r.enriched) == 37


def test_limit_labels_part_and_quarantines_nothing_wrongly(tmp_path):
    r = run(tmp_path, LabelModel(), batch_size=40, limit=5)
    assert r.stats.labelled_now == 5
    assert set(r.quarantine["reason"]) == {"not labelled (--limit)"}
    r2 = run(tmp_path, LabelModel(), batch_size=40)  # resumes: only the other 30 are sent
    assert r2.stats.cache_hits == 5 and r2.stats.labelled_now == 30 and r2.quarantine.empty


def test_missing_items_are_retried_then_bad_ones_quarantined(tmp_path):
    bad = E.normalise("Produto ok, nada demais.")
    llm = LabelModel(drop_first=3, corrupt={bad})
    r = run(tmp_path, llm, batch_size=40)
    assert llm.calls[0] == 35 and r.stats.retried_texts == 4          # 3 dropped + 1 invalid
    assert len(r.quarantine) == 1 and "unknown sentiment" in r.quarantine["reason"].iloc[0]
    assert len(r.enriched) == 36


def test_a_failed_request_quarantines_its_batch_with_the_reason(tmp_path):
    class Down:
        model = "down"

        def generate_json(self, system, prompt):
            raise LlmError("Gemini API 429: rate limit or quota reached")
    r = run(tmp_path, Down(), batch_size=40)
    assert len(r.quarantine) == 37 and r.quarantine_share == 1.0
    assert r.quarantine["reason"].str.contains("429").all() and r.stats.errors


@pytest.mark.parametrize("answer, reason", [
    ({"results": [{"i": 1, "s": "pos", "t": ["teleport"], "f": False}]}, "unknown topics"),
    ({"results": [{"i": 1, "s": "pos", "t": ["other"], "f": "yes"}]}, "true/false"),
    ({"results": [{"i": 1, "s": "pos", "t": ["other"], "f": False}] * 2}, "twice"),
    ({"labels": []}, "no results list"),
    ({"results": []}, "missing"),
])
def test_answer_checks(answer, reason):
    good, bad = E.check_answer(answer, 1, summaries=False)
    assert not good and reason in bad[1]


def test_answer_check_accepts_a_topic_string_and_orders_topics():
    good, bad = E.check_answer({"results": [{"i": 1, "s": "neg", "t": "damaged", "f": True},
                                            {"i": 2, "s": "pos", "t": ["recommendation", "delivery_speed"],
                                             "f": False, "e": "Fast."}]}, 2, summaries=True)
    assert not bad and good[1]["t"] == ["damaged"] and good[2]["t"] == ["delivery_speed", "recommendation"]
    assert good[2]["e"] == "Fast."


def test_cache_key_depends_on_model_and_prompt_but_not_case():
    assert E.cache_key("Muito bom!", "m", False) == E.cache_key("muito  bom", "m", False)
    assert E.cache_key("Muito bom", "m", False) != E.cache_key("Muito bom", "other", False)
    assert E.cache_key("Muito bom", "m", False) != E.cache_key("Muito bom", "m", True)


def test_cache_survives_a_half_written_line(tmp_path):
    path = tmp_path / "cache.jsonl"
    path.write_text('{"k": "a", "s": "pos", "t": ["other"], "f": false}\n{"k": "b", "s"', encoding="utf-8")
    assert E.Cache(path).get("a") and E.Cache(path).get("b") is None


def test_rate_limiter_spaces_request_starts():
    now, slept = [0.0], []
    limiter = E.RateLimiter(rpm=6, clock=lambda: now[0], sleep=lambda s: slept.append(s))
    for _ in range(3):
        limiter.wait()
    assert slept == [10.0, 20.0]
    E.RateLimiter(0, sleep=lambda s: pytest.fail("no limit, no sleep")).wait()


def test_star_agreement_and_baseline():
    acc, n, matrix = E.agreement(["positive", "negative", "neutral", "positive"], ["5", "1", "4", "x"])
    assert (acc, n) == (round(2 / 3, 3), 3) and matrix["positive"]["neutral"] == 1
    assert E.baseline_sentiment("Não recomendo, produto quebrado") == "negative"
    assert E.baseline_sentiment("Ótimo, chegou rápido") == "positive"


def test_plan_estimates_before_spending(tmp_path):
    p, todo = E.plan(E.load_reviews(E.SAMPLE_FILE), E.Cache(None), "m", 10, False, rpm=2)
    assert (p.to_label, p.requests, p.minutes_at_rpm) == (35, 4, 2.0) and len(todo) == 35
    assert p.input_tokens > 0 and p.output_tokens > 0


# --------------------------------------------------------------------------- the provider layer

def test_parse_json_tolerates_thinking_fences_and_chatter():
    assert L.parse_json('<think>hmm</think>\n```json\n{"a": 1}\n```') == {"a": 1}
    assert L.parse_json('Here you go: {"a": 2} hope it helps') == {"a": 2}
    with pytest.raises(LlmError):
        L.parse_json("no json here")


def test_openai_compatible_request_and_response():
    seen = {}

    def transport(request, timeout):
        seen.update(url=request.full_url, body=json.loads(request.data), auth=request.get_header("Authorization"))
        return json.dumps({"choices": [{"message": {"content": '{"results": []}'}}],
                           "usage": {"total_tokens": 7}}).encode()
    llm = L.OpenAICompatible("http://localhost:8080/v1/", "qwen", key="t", transport=transport)
    answer, usage = llm.generate_json("sys", "hello")
    assert answer == {"results": []} and usage == {"totalTokenCount": 7}
    assert seen["url"] == "http://localhost:8080/v1/chat/completions" and seen["auth"] == "Bearer t"
    assert seen["body"]["messages"][0] == {"role": "system", "content": "sys"}
    assert seen["body"]["temperature"] == 0.0 and seen["body"]["response_format"] == {"type": "json_object"}


def test_openai_compatible_explains_an_unreachable_server():
    def transport(request, timeout):
        raise urllib.error.URLError("connection refused")
    with pytest.raises(LlmError, match="tunnel"):
        L.OpenAICompatible("http://localhost:8080/v1", "m", transport=transport).generate_text("s", "p")

    def missing(request, timeout):
        raise urllib.error.HTTPError("x", 404, "nf", {}, io.BytesIO(b"{}"))
    with pytest.raises(LlmError, match="ollama pull"):
        L.OpenAICompatible("http://x/v1", "m", transport=missing).generate_text("s", "p")


def test_make_llm_picks_the_backend(monkeypatch):
    for var in ("ECOM_LLM_PROVIDER", "ECOM_LLM_MODEL", "ECOM_LLM_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    g = L.make_llm()
    assert isinstance(g, L.GeminiJson) and g.temperature == 0.0
    assert type(L.make_llm(purpose="ask")).__name__ == "Gemini"
    o = L.make_llm("ollama")
    assert (o.base_url, o.model) == (L.OLLAMA_URL, L.OLLAMA_MODEL)
    with pytest.raises(ValueError, match="ECOM_LLM_BASE_URL"):
        L.make_llm("openai")
    monkeypatch.setenv("ECOM_LLM_PROVIDER", "openai")
    monkeypatch.setenv("ECOM_LLM_BASE_URL", "http://localhost:8080/v1")
    assert L.make_llm().base_url == "http://localhost:8080/v1"
    with pytest.raises(ValueError, match="unknown"):
        L.make_llm("gpt")


def test_cli_runs_the_sample_end_to_end(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(L, "make_llm", lambda *a, **k: LabelModel())
    monkeypatch.setattr(L, "describe", lambda llm: "stand-in")
    monkeypatch.setattr(E.config, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.delenv("ECOM_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("ECOM_LLM_MODEL", raising=False)
    monkeypatch.setenv("GEMINI_MODEL", "stand-in")  # the dry run keys the cache by the configured model
    assert main(["enrich-reviews", "--source", "sample", "--out", str(tmp_path / "out")]) == 0
    md = (tmp_path / "reports" / "enrich_sample.md").read_text(encoding="utf-8")
    assert "Is the model worth it?" in md and "Against hand labels (37 reviews): sentiment 100%" in md
    assert main(["enrich-reviews", "--source", "sample", "--dry-run", "--out", str(tmp_path / "out")]) == 0
    assert "35 already cached" in capsys.readouterr().out
