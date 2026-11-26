"""Step 11: label customer reviews with a language model, cheaply enough to be worth it.

Olist's ``order_reviews`` has a 1-5 star score and, for some reviews, a comment
in Portuguese. The stars already say *how* happy a customer was; the comment
says *why* ("chegou atrasado", "veio quebrado", "produto errado"). This step
turns each comment into three columns a dashboard or SQL can use:

- ``sentiment``  positive / neutral / negative
- ``topics``     one or more of TOPICS (delivery speed, damaged, wrong item, ...)
- ``follow_up``  does the customer still need something (a refund, a missing item)?

A model call is the most expensive operation in the whole pipeline (seconds, not
microseconds), so the step is built around not making calls:

1. **Only rows with text.** Reviews without a comment are skipped.
2. **Deduplicate.** Identical comments ("Muito bom", "Recomendo") are labelled once.
3. **Cache.** Every label is appended to ``cache.jsonl`` keyed by a hash of
   (prompt version, model, text). A rerun, or a run on new data, only pays for
   texts it has never seen; an interrupted run resumes where it stopped.
4. **Batch.** Many comments per request, with short JSON keys, so a free-tier
   quota of requests per day covers the whole dataset.
5. **Estimate first.** ``--dry-run`` prints requests, tokens and time before
   anything is spent.

Every answer is checked: each comment must come back exactly once, with a known
sentiment and topics. Failures are retried once in smaller batches, then go to
``quarantine.csv`` with a reason, like bad rows in step 3. Quality is measured
two ways: agreement with the star score (free ground truth on every review), and
against hand labels for the bundled sample. A keyword baseline runs alongside, so
the report shows what the model adds over a rule that costs nothing.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from . import config

REVIEWS_FILE = "olist_order_reviews_dataset.csv"
SAMPLE_FILE = config.PROJECT_ROOT / "docs" / "samples" / "reviews_sample.csv"
SAMPLE_LABELS = config.PROJECT_ROOT / "docs" / "samples" / "reviews_sample_labels.csv"

PROMPT_VERSION = "v1"  # change it when the prompt or labels change: the cache then starts afresh
SENTIMENTS = {"pos": "positive", "neu": "neutral", "neg": "negative"}
TOPICS = ("delivery_speed", "not_received", "damaged", "wrong_or_missing_item", "product_quality",
          "seller_service", "price_value", "recommendation", "other")

SYSTEM_PROMPT = f"""You label Brazilian Portuguese customer reviews from an online marketplace.
For every review return one object with:
- "i": the review's number, exactly as given;
- "s": sentiment of the text, one of "pos", "neu", "neg";
- "t": list of 1 to 3 topics the text is about, from: {", ".join(TOPICS)};
  delivery_speed = fast or late delivery; not_received = never arrived; wrong_or_missing_item = wrong
  product, wrong colour or size, or part of the order missing; recommendation = says they recommend
  (or not) the store or product; other = none of these;
- "f": true if the customer still needs action (waiting for the product, asks for a refund, exchange
  or contact), otherwise false.{{summary_rule}}
Return JSON only: {{{{"results": [...]}}}} with one object per review, no other text."""
SUMMARY_RULE = '\n- "e": an English summary of at most 12 words.'


# --------------------------------------------------------------------------- input

def reviews_path(source: str) -> Path:
    if source == "sample":
        return SAMPLE_FILE
    path = Path(source)
    if path.suffix == ".csv":
        return path
    found = config.source_dir(source) / REVIEWS_FILE
    if not found.exists() and source == "synthetic":
        raise FileNotFoundError("the synthetic data has no reviews: use --source sample (40 bundled reviews) "
                                "or --source olist after download-olist")
    return found


def load_reviews(path: Path) -> pd.DataFrame:
    """Olist-shaped reviews with a ``text`` column: title and comment joined, whitespace tidied."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = {"review_id", "order_id", "review_score", "review_comment_message"} - set(df.columns)
    if missing:
        raise ValueError(f"{path} is not an Olist reviews file (missing {sorted(missing)})")
    title = df.get("review_comment_title", pd.Series("", index=df.index))
    df["text"] = [" ".join(f"{t}. {m}".strip(" .").split()) if t.strip() else " ".join(m.split())
                  for t, m in zip(title, df["review_comment_message"])]
    return df


def normalise(text: str) -> str:
    """The deduplication key: case and spacing ignored, trailing punctuation dropped."""
    return re.sub(r"[\s.!]+$", "", " ".join(text.casefold().split()))


def cache_key(text: str, model: str, summaries: bool) -> str:
    raw = f"{PROMPT_VERSION}|{model}|{int(summaries)}|{normalise(text)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


# --------------------------------------------------------------------------- prompt and answer

def build_prompt(texts: Sequence[str]) -> str:
    lines = [f"{i}. {json.dumps(t, ensure_ascii=False)}" for i, t in enumerate(texts, start=1)]
    return f"Label these {len(texts)} reviews:\n" + "\n".join(lines)


def system_prompt(summaries: bool) -> str:
    return SYSTEM_PROMPT.replace("{summary_rule}", SUMMARY_RULE if summaries else "").replace("{{", "{") \
        .replace("}}", "}")


def check_answer(answer: dict, n: int, summaries: bool) -> Tuple[Dict[int, dict], Dict[int, str]]:
    """Split the model's answer into valid labels (by position 1..n) and reasons for the rest."""
    results = answer.get("results") if isinstance(answer, dict) else None
    good: Dict[int, dict] = {}
    bad: Dict[int, str] = {}
    if not isinstance(results, list):
        return good, {i: "answer has no results list" for i in range(1, n + 1)}
    seen = set()
    for item in results:
        if not isinstance(item, dict):
            continue
        try:
            i = int(item.get("i"))
        except (TypeError, ValueError):
            continue
        if not 1 <= i <= n:
            continue
        if i in seen:
            bad[i] = "labelled twice"
            good.pop(i, None)
            continue
        seen.add(i)
        s, t, f = item.get("s"), item.get("t"), item.get("f")
        if isinstance(t, str):
            t = [t]
        if s not in SENTIMENTS:
            bad[i] = f"unknown sentiment {s!r}"
        elif not isinstance(t, list) or not t or any(x not in TOPICS for x in t):
            bad[i] = f"unknown topics {t!r}"
        elif not isinstance(f, bool):
            bad[i] = f"follow-up flag is not true/false: {f!r}"
        else:
            label = {"s": s, "t": sorted(set(t), key=TOPICS.index)[:3], "f": f}
            if summaries:
                label["e"] = str(item.get("e", "")).strip()[:120]
            good[i] = label
    for i in range(1, n + 1):
        if i not in good and i not in bad:
            bad[i] = "missing from the answer"
    return good, bad


# --------------------------------------------------------------------------- cache

class Cache:
    """Labels by key, in memory, appended to a JSONL file after every batch (so a crash loses one batch)."""

    def __init__(self, path: Optional[Path]):
        self.path, self.labels, self._lock = path, {}, threading.Lock()
        if path and path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    entry = json.loads(line)
                    self.labels[entry.pop("k")] = entry
                except (json.JSONDecodeError, KeyError):
                    continue  # a half-written last line from an interrupted run

    def get(self, key: str) -> Optional[dict]:
        return self.labels.get(key)

    def put_many(self, entries: Dict[str, dict]) -> None:
        if not entries:
            return
        with self._lock:
            self.labels.update(entries)
            if self.path:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as f:
                    for key, label in entries.items():
                        f.write(json.dumps({"k": key, **label}, ensure_ascii=False) + "\n")


class RateLimiter:
    """At most ``rpm`` request starts per minute across threads; 0 means no limit (a local server)."""

    def __init__(self, rpm: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.interval = 60.0 / rpm if rpm else 0.0
        self.clock, self.sleep, self._next, self._lock = clock, sleep, 0.0, threading.Lock()

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:
            now = self.clock()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            self.sleep(start - now)


# --------------------------------------------------------------------------- running

@dataclass
class Plan:
    rows: int
    with_text: int
    unique_texts: int
    cached: int
    to_label: int
    requests: int
    input_tokens: int
    output_tokens: int
    minutes_at_rpm: Optional[float]


@dataclass
class Stats:
    model: str
    rows: int = 0
    with_text: int = 0
    unique_texts: int = 0
    cache_hits: int = 0
    labelled_now: int = 0
    requests: int = 0
    retried_texts: int = 0
    quarantined_texts: int = 0
    tokens: int = 0
    llm_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)

    @property
    def texts_per_second(self) -> float:
        return round(self.labelled_now / self.llm_seconds, 2) if self.llm_seconds else 0.0


def _approx_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 3.5))  # Portuguese runs at ~3-4 characters per token


def plan(df: pd.DataFrame, cache: Cache, model: str, batch_size: int, summaries: bool,
         rpm: float = 0.0, limit: Optional[int] = None) -> Tuple[Plan, List[str]]:
    """What a run would cost, and the unique texts it would send (in first-seen order)."""
    texts = df.loc[df["text"] != "", "text"]
    unique: Dict[str, str] = {}
    for t in texts:
        unique.setdefault(normalise(t), t)
    todo = [t for t in unique.values() if cache.get(cache_key(t, model, summaries)) is None]
    if limit is not None:
        todo = todo[:limit]
    requests = math.ceil(len(todo) / batch_size) if todo else 0
    system = _approx_tokens(system_prompt(summaries))
    inp = requests * (system + 12) + sum(_approx_tokens(t) + 4 for t in todo)
    out = len(todo) * (18 + (20 if summaries else 0)) + requests * 6
    minutes = round(requests / rpm, 1) if rpm else None
    return Plan(len(df), len(texts), len(unique), len(unique) - len(
        [t for t in unique.values() if cache.get(cache_key(t, model, summaries)) is None]),
        len(todo), requests, inp, out, minutes), todo


def _chunks(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield list(items[i:i + size])


def label_texts(texts: Sequence[str], llm, model: str, cache: Cache, stats: Stats, batch_size: int = 40,
                workers: int = 1, limiter: Optional[RateLimiter] = None, summaries: bool = False,
                progress: Optional[Callable[[int, int], None]] = None) -> Dict[str, str]:
    """Label texts not in the cache. Returns {text: quarantine reason} for those that failed twice."""
    limiter = limiter or RateLimiter(0)
    system = system_prompt(summaries)
    lock = threading.Lock()
    done = [0]

    def run_batch(batch: List[str]) -> Dict[str, str]:
        limiter.wait()
        t0 = time.perf_counter()
        try:
            answer, usage = llm.generate_json(system, build_prompt(batch))
            good, bad = check_answer(answer, len(batch), summaries)
        except Exception as e:  # noqa: BLE001  network, quota, unparseable JSON: the whole batch failed
            good, usage = {}, {}
            bad = {i: f"request failed: {str(e)[:150]}" for i in range(1, len(batch) + 1)}
            with lock:
                stats.errors.append(str(e)[:200])
        elapsed = time.perf_counter() - t0
        cache.put_many({cache_key(batch[i - 1], model, summaries): label for i, label in good.items()})
        with lock:
            stats.requests += 1
            stats.tokens += int(usage.get("totalTokenCount", 0) or 0)
            stats.labelled_now += len(good)
            stats.llm_seconds += elapsed / max(1, workers)  # wall-clock share when requests overlap
            done[0] += len(batch)
            if progress:
                progress(done[0], total)
        return {batch[i - 1]: reason for i, reason in bad.items()}

    def run_all(items: List[str], size: int) -> Dict[str, str]:
        failed: Dict[str, str] = {}
        batches = list(_chunks(items, size))
        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for result in pool.map(run_batch, batches):
                    failed.update(result)
        else:
            for batch in batches:
                failed.update(run_batch(batch))
        return failed

    total = len(texts)
    failed = run_all(list(texts), batch_size)
    if failed:  # one retry, in batches a quarter of the size: long batches are where models slip
        stats.retried_texts += len(failed)
        total += len(failed)
        failed = run_all(list(failed), max(1, batch_size // 4))
    stats.quarantined_texts += len(failed)
    return failed


def assemble(df: pd.DataFrame, cache: Cache, model: str, summaries: bool,
             failed: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """One row per review with text: the labels, or a quarantine row with the reason."""
    rows, quarantine = [], []
    failed_norm = {normalise(t): r for t, r in failed.items()}
    for rec in df.loc[df["text"] != ""].itertuples(index=False):
        label = cache.get(cache_key(rec.text, model, summaries))
        base = {"review_id": rec.review_id, "order_id": rec.order_id, "review_score": rec.review_score}
        if label is None:
            reason = failed_norm.get(normalise(rec.text), "not labelled (--limit)")
            quarantine.append({**base, "text": rec.text, "reason": reason})
            continue
        row = {**base, "sentiment": SENTIMENTS[label["s"]], "topics": "|".join(label["t"]),
               "follow_up": label["f"], "baseline_sentiment": baseline_sentiment(rec.text),
               "text": rec.text}
        if summaries:
            row["summary_en"] = label.get("e", "")
        rows.append(row)
    return pd.DataFrame(rows), pd.DataFrame(quarantine, columns=["review_id", "order_id", "review_score",
                                                                 "text", "reason"])


# --------------------------------------------------------------------------- measuring

NEGATIONS = ("não recomendo", "nao recomendo", "não gostei", "nao gostei", "não chegou", "nao chegou",
             "não recebi", "nao recebi", "não funciona", "nao funciona", "não veio", "nao veio")
NEGATIVE = ("atras", "demor", "defeit", "quebrad", "danific", "errad", "faltou", "falta", "péssim", "pessim",
            "ruim", "horrível", "horrivel", "decepcion", "devolu", "cancel", "insatisf", "problema", "nunca",
            "reembols", "absurdo", "lament")
POSITIVE = ("ótim", "otim", "excelente", "bom", "boa", "recomendo", "perfeit", "rápid", "rapid",
            "antes do prazo", "satisfeit", "adorei", "amei", "maravilh", "parabéns", "parabens", "super",
            "gostei", "lindo", "certinho", "no prazo")


def baseline_sentiment(text: str) -> str:
    """A keyword rule: the free alternative the model has to beat."""
    t = text.casefold()
    neg = sum(t.count(w) for w in NEGATIONS) * 2
    rest = t
    for w in NEGATIONS:
        rest = rest.replace(w, " ")
    neg += sum(rest.count(w) for w in NEGATIVE)
    pos = sum(rest.count(w) for w in POSITIVE)
    return "positive" if pos > neg else "negative" if neg > pos else "neutral"


def star_sentiment(score) -> Optional[str]:
    try:
        s = int(score)
    except (TypeError, ValueError):
        return None
    return "negative" if s <= 2 else "neutral" if s == 3 else "positive"


def agreement(labels: Sequence[str], stars: Sequence) -> Tuple[float, int, Dict[str, Dict[str, int]]]:
    """Share of reviews whose text sentiment matches the stars (1-2 negative, 3 neutral, 4-5 positive),
    and the confusion matrix (rows: stars, columns: label)."""
    names = ("negative", "neutral", "positive")
    matrix = {a: {b: 0 for b in names} for a in names}
    hits = n = 0
    for label, score in zip(labels, stars):
        want = star_sentiment(score)
        if want is None or label not in names:
            continue
        matrix[want][label] += 1
        hits += want == label
        n += 1
    return (round(hits / n, 3) if n else 0.0), n, matrix


def golden_agreement(out: pd.DataFrame, labels_path: Path) -> Optional[dict]:
    """Sentiment accuracy and mean topic overlap (Jaccard) against hand labels."""
    if not labels_path.exists() or out.empty:
        return None
    gold = pd.read_csv(labels_path, dtype=str, keep_default_na=False)
    merged = out.merge(gold, on="review_id", suffixes=("", "_gold"))
    if merged.empty:
        return None
    sent = (merged["sentiment"] == merged["sentiment_gold"]).mean()
    base = (merged["baseline_sentiment"] == merged["sentiment_gold"]).mean()
    jac = []
    for a, b in zip(merged["topics"], merged["topics_gold"]):
        sa, sb = set(a.split("|")) - {""}, set(b.split("|")) - {""}
        jac.append(len(sa & sb) / len(sa | sb) if sa | sb else 1.0)
    follow = (merged["follow_up"].astype(str).str.lower() == merged["follow_up_gold"].str.lower()).mean()
    return {"n": len(merged), "sentiment": round(float(sent), 3), "baseline_sentiment": round(float(base), 3),
            "topic_jaccard": round(sum(jac) / len(jac), 3), "follow_up": round(float(follow), 3)}


# --------------------------------------------------------------------------- the whole step

@dataclass
class Result:
    stats: Stats
    plan: Plan
    out_dir: Path
    enriched: pd.DataFrame
    quarantine: pd.DataFrame
    star_agreement: float
    baseline_star_agreement: float
    compared: int
    matrix: Dict[str, Dict[str, int]]
    golden: Optional[dict]

    @property
    def quarantine_share(self) -> float:
        total = len(self.enriched) + len(self.quarantine)
        return round(len(self.quarantine) / total, 4) if total else 0.0


def enrich_reviews(path: Path, llm, model: str, out_dir: Path, batch_size: int = 40, workers: int = 1,
                   rpm: float = 0.0, summaries: bool = False, limit: Optional[int] = None,
                   labels_path: Optional[Path] = None, progress=None) -> Result:
    df = load_reviews(path)
    cache = Cache(out_dir / "cache.jsonl")
    the_plan, todo = plan(df, cache, model, batch_size, summaries, rpm, limit)
    stats = Stats(model=model, rows=the_plan.rows, with_text=the_plan.with_text,
                  unique_texts=the_plan.unique_texts, cache_hits=the_plan.cached)
    failed = label_texts(todo, llm, model, cache, stats, batch_size, workers, RateLimiter(rpm), summaries,
                         progress) if todo else {}
    enriched, quarantine = assemble(df, cache, model, summaries, failed)
    out_dir.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_dir / "reviews_enriched.csv", index=False)
    quarantine.to_csv(out_dir / "quarantine.csv", index=False)
    acc, n, matrix = agreement(enriched.get("sentiment", []), enriched.get("review_score", []))
    base_acc, _, _ = agreement(enriched.get("baseline_sentiment", []), enriched.get("review_score", []))
    golden = golden_agreement(enriched, labels_path) if labels_path else None
    result = Result(stats, the_plan, out_dir, enriched, quarantine, acc, base_acc, n, matrix, golden)
    (out_dir / "run.json").write_text(json.dumps({"stats": asdict(stats), "plan": asdict(the_plan),
                                                  "texts_per_second": stats.texts_per_second,
                                                  "star_agreement": acc, "baseline_star_agreement": base_acc,
                                                  "golden": golden}, indent=2), encoding="utf-8")
    return result


def report(result: Result, source: str, where: str) -> str:
    s, p = result.stats, result.plan
    lines = [f"# Review enrichment: `{source}`", "",
             f"Model: {where}. Prompt {PROMPT_VERSION}.", "",
             "## Cost", "",
             "| | count |", "|---|---:|",
             f"| reviews | {s.rows:,} |",
             f"| with a comment | {s.with_text:,} |",
             f"| unique comments | {s.unique_texts:,} |",
             f"| already in the cache | {s.cache_hits:,} |",
             f"| labelled in this run | {s.labelled_now:,} |",
             f"| requests | {s.requests:,} |",
             f"| retried / quarantined | {s.retried_texts:,} / {s.quarantined_texts:,} |",
             f"| tokens | {s.tokens:,} |",
             f"| model time | {s.llm_seconds:.1f} s |",
             f"| throughput | {s.texts_per_second} comments/s |", ""]
    saved = 1 - (p.unique_texts / p.with_text) if p.with_text else 0
    lines += [f"Deduplication removed {saved:.0%} of the comments before any request.", ""]
    lines += ["## Quality", "",
              "Agreement of the text's sentiment with the star score (1-2 negative, 3 neutral, 4-5 positive), "
              f"on {result.compared:,} reviews:", "",
              "| method | agreement with stars |", "|---|---:|",
              f"| model | {result.star_agreement:.1%} |",
              f"| keyword baseline (free) | {result.baseline_star_agreement:.1%} |", "",
              "Rows: stars. Columns: the model's label.", "",
              "| stars \\ model | negative | neutral | positive |", "|---|---:|---:|---:|"]
    for want, row in result.matrix.items():
        lines.append(f"| {want} | {row['negative']} | {row['neutral']} | {row['positive']} |")
    if result.golden:
        g = result.golden
        lines += ["", f"Against hand labels ({g['n']} reviews): sentiment {g['sentiment']:.0%} "
                      f"(keyword baseline {g['baseline_sentiment']:.0%}), topics {g['topic_jaccard']:.0%} "
                      f"mean overlap, follow-up flag {g['follow_up']:.0%}."]
    if not result.enriched.empty:
        topics = result.enriched["topics"].str.split("|").explode().value_counts()
        lines += ["", "## Topics", "", "| topic | comments |", "|---|---:|"]
        lines += [f"| {t} | {c:,} |" for t, c in topics.items()]
        neg = result.enriched[result.enriched["sentiment"] == "negative"]
        if not neg.empty:
            top = neg["topics"].str.split("|").explode().value_counts().head(3)
            lines += ["", "Most common reasons in negative comments: "
                      + ", ".join(f"{t} ({c:,})" for t, c in top.items()) + "."]
        lines += [f"", f"{int(result.enriched['follow_up'].astype(bool).sum()):,} comments still need action."]
    lines += ["", "## Is the model worth it?", "",
              "Sentiment alone is not: the stars already give it for free, and the keyword rule costs nothing. "
              "The model earns its cost with what neither has: the topics (why customers are unhappy) and the "
              "follow-up flag. Judge it on those, against the time per 1,000 comments above."]
    if s.errors:
        lines += ["", "## Errors", ""] + [f"- {e}" for e in s.errors[:5]]
    return "\n".join(lines) + "\n"
