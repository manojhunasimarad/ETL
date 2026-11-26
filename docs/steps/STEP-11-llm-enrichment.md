# Step 11: LLM enrichment of review comments

**Goal:** add a column no SQL can compute (*why* a customer is unhappy) from
free text, at a cost you can state and defend, with the same checks every other
step has.

**JD lines this answers:** GenAI in a data pipeline; data quality; "optimize
pipelines for cost and performance".

---

## 1. Concepts

### 1.1 Stars say how, comments say why

`order_reviews` has a 1-5 score and, often, a Portuguese comment. The score is a
number: SQL can average it. The comment is text: "chegou atrasado", "veio
quebrado", "comprei azul e veio vermelho". A model turns it into columns:

| column | values |
|---|---|
| `sentiment` | positive, neutral, negative |
| `topics` | delivery_speed, not_received, damaged, wrong_or_missing_item, product_quality, seller_service, price_value, recommendation, other |
| `follow_up` | true if the customer still needs something |

Then "which sellers cause the most *damaged* complaints?" is a GROUP BY.

### 1.2 The expensive call

A Spark row costs microseconds. A model call costs a second or more, and the
free tier caps calls per day. So the design question is **how few calls**:

1. only rows with a comment;
2. **deduplicate**: identical comments ("Muito bom", "Recomendo") are labelled once;
3. **cache** by hash of (prompt version, model, normalised text): reruns and new
   data pay only for unseen text, and an interrupted run resumes;
4. **batch**: 40 comments per Gemini request with one-letter JSON keys
   (`"s"`, `"t"`, `"f"`), because output tokens are the slow part;
5. **estimate first**: `--dry-run`.

These are the same ideas as steps 7 and 9: avoid recomputation (cache),
idempotent reruns (the cache key), and incremental loads (only new text).

### 1.3 Never trust the answer

The model's JSON is checked like a bronze row in step 3: every comment must come
back exactly once (`"i"` is its position in the batch), with a known sentiment,
1-3 known topics and a true/false flag. Failures are retried once in batches a
quarter of the size (long answers are where models drop items), then go to
`quarantine.csv` with a reason. A gate fails the run if more than 2% are
quarantined.

### 1.4 Is it worth it? Measure against free

Two free references:

- the **star score**: 1-2 negative, 3 neutral, 4-5 positive. Agreement with it is
  a quality measure on every review, no labelling needed. It is not ground
  truth: "Gostei, mas a embalagem veio amassada" can be 4 stars and still name
  a problem.
- a **keyword baseline** (`baseline_sentiment`): 0 seconds, no quota.

If the model only matched the stars on sentiment, it would be wasted money. It
earns its cost with topics and follow-up, which neither free method gives. The
report says this explicitly, and the 40 hand-labelled sample reviews
(`docs/samples/reviews_sample_labels.csv`) measure topics too.

### 1.5 One client, many backends

`llm.py` has two clients: Gemini (hosted) and any **OpenAI-compatible** server
(Ollama, llama.cpp, vLLM). The second means the same code runs on Kaggle's free
T4s and on the college V100 by changing a URL. Defaults differ on purpose: a
hosted API is limited by requests per minute (big batches, one at a time), a
local server by the GPU (smaller batches, four in parallel, which the server
batches on the GPU). See [LLM.md](../LLM.md).

## 2. Code tour

| File | What to read |
|---|---|
| `src/ecom_etl/llm.py` | `OpenAICompatible.generate_text`, `parse_json` (strips `<think>` and fences), `make_llm` |
| `src/ecom_etl/enrich.py` | `plan` (dedupe + cost), `label_texts` (batches, threads, retry), `check_answer`, `Cache`, `RateLimiter`, `agreement` |
| `src/ecom_etl/cli.py` | `_cmd_enrich_reviews`, `ENRICH_DEFAULTS` |
| `tests/test_enrich.py` | a stand-in model that answers from the hand labels: every path tested without quota |
| `cloud/kaggle/enrich_reviews.ipynb` | the full run on free GPUs |

## 3. Run it

```powershell
python -m ecom_etl enrich-reviews --source sample --dry-run
python -m ecom_etl enrich-reviews --source sample          # Gemini, 1 request
python -m ecom_etl enrich-reviews --source sample          # again: 0 requests (cache)
python -m ecom_etl enrich-reviews --source olist --dry-run # after download-olist
```

The full Olist run: Kaggle notebook (or the V100), as in [LLM.md](../LLM.md).

## 4. Exercises

1. Run the sample twice. Where does the second run's time go?
2. Change `PROMPT_VERSION` to `v2` and run again. Why does everything get relabelled, and why is that correct?
3. Run the sample with `--batch-size 5` and with `--batch-size 40`. Compare requests, tokens and seconds.
4. Add a topic `packaging`. Which files change? Which hand labels? What happens to the cache?
5. Find three sample reviews where the keyword baseline is wrong. What would a better rule need, and when would the rule be good enough to drop the model?
6. On the Olist report, read the confusion matrix. Which cell is largest off the diagonal? Read 10 of those reviews: is the model wrong, or the stars?

## 5. Check yourself

1. Why is the cache key a hash of the prompt version, the model and the normalised text, and not the review_id?
2. What does `--limit` give you, and why is a limited run not wasted?
3. Why does the retry use a smaller batch?
4. Why is agreement with the star score not accuracy?
5. Why 4 workers for a local server but 1 for Gemini?
6. When would you not use a model at all here?

## 6. Learning material

**O'Reilly**
- *AI Engineering* (Chip Huyen, 2025): ch. 3 "Evaluation Methodology" and ch. 9 "Inference Optimization" (batching, caching, cost).
- *Prompt Engineering for LLMs* (Berryman and Ziegler): structured output and few-shot prompts.
- *Designing Machine Learning Systems* (Huyen): ch. 4 on labels and weak supervision (the star score is one).

**Documentation**
- Gemini API: rate limits, structured output (`responseMimeType`, response schemas).
- Ollama: the OpenAI-compatible endpoint, `OLLAMA_NUM_PARALLEL`.
- llama.cpp server: `--parallel`, continuous batching.

## 7. Done when

- [ ] the sample run printed a report and a second run made 0 requests
- [ ] the Olist run is done (Kaggle or V100) and its throughput, quarantine share and star agreement are rows in [RESULTS.md](../RESULTS.md)
- [ ] you can answer section 5 without the notes
- [ ] step 11 has a story in [STORIES.md](../STORIES.md)
