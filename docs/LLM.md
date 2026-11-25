# LLMs in this project: where they run and what they cost

Two features use a language model:

| Feature | Command | What the model does |
|---|---|---|
| Ask your data ([ASK.md](ASK.md)) | `ask "..."`, `ask-eval` | writes one SQL query from the schema; your rows never leave Postgres |
| Review enrichment ([step 11](steps/STEP-11-llm-enrichment.md)) | `enrich-reviews` | labels review comments: sentiment, topics, follow-up |

Both go through [`src/ecom_etl/llm.py`](../src/ecom_etl/llm.py), so the same
command runs on any backend. Pick one with `--llm` or `ECOM_LLM_PROVIDER` in `.env`.

## Which backend, when

The rule: **a model call is the most expensive thing in the pipeline. Spend it
where it adds something a free method cannot, and measure that it does.**

| Backend | Runs on | Cost | Use it for |
|---|---|---|---|
| `gemini` (default) | Google's servers | free tier: limited requests per minute and per day | development, `ask`, the 40-review sample, small batches |
| `ollama` on **Kaggle** | 2 × T4 (16 GB each), free ~30 GPU-hours a week | your quota | the full Olist review run |
| `openai` on the **V100** | college server, 32 GB, through an SSH tunnel | free | the full run and benchmarks, once the driver is fixed |
| `ollama` on the laptop | GTX 1650, 4 GB | slows the whole machine | not recommended: only tiny models fit, with 8 GB RAM shared with Docker |

## Gemini (nothing to install)

```powershell
# .env (git-ignored)
GEMINI_API_KEY=...
python -m ecom_etl enrich-reviews --source sample --dry-run   # cost first
python -m ecom_etl enrich-reviews --source sample              # 1 request for 35 unique comments
```

The free tier counts requests, not comments, so `enrich-reviews` packs 40
comments into each request and waits between requests (`--rpm 8`). Check your
limits in AI Studio (Rate limit page) before a big run: `--dry-run` prints how
many requests it needs.

## Kaggle (free T4 GPUs)

Import [`cloud/kaggle/enrich_reviews.ipynb`](../cloud/kaggle/enrich_reviews.ipynb)
(Kaggle → Create → Import notebook), set **GPU T4 x2** and **Internet on**, add
the dataset `olistbr/brazilian-ecommerce`, and *Run all*. It starts Ollama with
`qwen2.5:7b-instruct`, installs this repository from GitHub and runs the same
command. Download `reviews_enriched.csv`, `cache.jsonl` and `enrich_olist.md`
from the Output panel into `data/enriched/olist/` and `reports/`.

Copying `cache.jsonl` home matters: the labels are keyed by model and text, so a
later run with the same model labels only new reviews.

## V100 (college server)

The V100 is Volta (compute capability 7.0). Recent vLLM releases no longer ship
kernels for it, so use llama.cpp's server, which still builds for it. Because you
are in the `docker` group, no sudo is needed:

```bash
# on the server; bind to 127.0.0.1 so the shared machine does not expose it
docker run -d --name llm --gpus '"device=0"' -p 127.0.0.1:8080:8080 \
  -v ~/models:/root/.cache/llama.cpp \
  ghcr.io/ggml-org/llama.cpp:server-cuda \
  -hf bartowski/Qwen2.5-7B-Instruct-GGUF:Q4_K_M --host 0.0.0.0 --port 8080 -ngl 99 -c 16384 --parallel 4
docker logs -f llm      # "no kernel image is available" = the image lacks sm_70: build with -DCMAKE_CUDA_ARCHITECTURES=70
```

```powershell
# on the laptop: a tunnel, then the same command
ssh -N -L 8080:localhost:8080 v100
python -m ecom_etl enrich-reviews --source olist --llm openai --base-url http://localhost:8080/v1 --model qwen2.5-7b
```

First check that `ssh v100 nvidia-smi` prints the GPU table. "Driver/library
version mismatch" means the server still needs its reboot (see [GPU.md](GPU.md)).

## Tuning knobs (and what they trade)

| Flag | Default | Higher means |
|---|---|---|
| `--batch-size` | 40 Gemini, 10 local | fewer requests; but long answers are where models skip or mislabel items (they are retried) |
| `--workers` | 1 Gemini, 4 local | parallel requests; a local server batches them on the GPU. Match it to the server's parallel slots |
| `--rpm` | 8 Gemini, 0 local | faster, until the API answers 429 |
| `--summaries` | off | an English summary per comment: about twice the output tokens, so about twice the time |
| `--limit` | all | label only N new comments: a cheap trial before the full run |

Every run writes its throughput (comments per second), requests and tokens to
`run.json` and the report. Record them in [RESULTS.md](RESULTS.md) per machine.
