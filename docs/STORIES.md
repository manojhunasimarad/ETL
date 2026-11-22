# Interview stories

One short story per step, written by me after the step is merged. Each
answers "tell me about a problem you solved in this project" in under a
minute. Numbers must come from [`RESULTS.md`](RESULTS.md).

Format:
- **Problem**: what was wrong or unknown, in one sentence
- **Action**: what I built or decided, and why that way
- **Result**: the measured outcome
- **Trade-off / next time**: what I would change, or the limitation

---

## Step 1: Data sources and profiling

- **Problem**: (fill in after profiling the real Olist data: which issues did it have that a naive pipeline would load silently?)
- **Action**: wrote a source contract for 7 tables, a profiler that reads everything as text, and a synthetic generator with ground-truth defects to test the profiler against.
- **Result**: (fill in: number of real Olist findings; synthetic: all 14 injected defect types detected with exact counts)
- **Trade-off / next time**: (fill in)
