# How each step is learned, built and merged

The goal is twofold: a working pipeline, **and** being able to explain
and defend every part of it in an interview. A quiz alone gives theory;
writing part of the code yourself is what lets you answer "walk me
through how you did X" with confidence.

## The loop (once per concept, a few concepts per step)

| # | Stage | Who | Output |
|---|---|---|---|
| 1 | **Explain** the concept, tied to the job requirement it answers | Claude | section of `docs/steps/STEP-NN-*.md` |
| 2 | **Predict**: before seeing any code, sketch the design (grain, keys, what can go wrong) | you | a few lines in chat |
| 3 | **Build**: scaffolding, tests and docs; one small piece is left for you | Claude, then you | commits on the step branch |
| 4 | **Run** it on your machine and read the real output | you | numbers, reports |
| 5 | **Quiz**: 5 questions on the concept *and on your own output*; pass is 4/5 | both | missed questions get re-explained and asked again next session |
| 6 | **Commit and merge**: CI must be green; you merge the pull request | you | `main` updated |
| 7 | **Story**: 3 lines in [`STORIES.md`](STORIES.md): problem, action, measured result | you | interview answer |

To get a quiz, say "quiz me on step N" in the chat.

## "Your turn" pieces

Code you are meant to write is marked `YOUR TURN` and raises
`NotImplementedError`. Its test **skips** (not fails) until you implement
it, so CI stays green. Your current to-do list:

```bash
pytest -rs          # the "SKIPPED ... your turn: ..." lines are your tasks
```

When you implement it, the skip turns into a pass (or a failure that
tells you what is wrong).

## Git conventions

* One branch and one pull request per step: `step-02-postgres`, `step-03-spark`, ...
* Small commits, each one passing `pytest`. Subject in the imperative,
  under 72 characters ("Add chunked raw loader"); body explains **why**.
* CI must be green before merging. You review the diff and press merge.
* Data is never committed (`data/` and `reports/` are git-ignored).

### Commands (PowerShell or bash)

```bash
# start of a session: get the latest main
git checkout main
git pull

# work on a step branch that Claude pushed
git fetch origin
git checkout step-02-postgres
git pull

# commit your "your turn" piece
pytest -q
git add src/ecom_etl/<file>.py
git commit -m "Implement <what you did>"
git push

# after merging the pull request on GitHub
git checkout main
git pull
```

## Where things live

| What | Where |
|---|---|
| Concepts, exercises, reading list | `docs/steps/STEP-NN-*.md` |
| Measured numbers for the resume | [`RESULTS.md`](RESULTS.md) |
| Interview stories | [`STORIES.md`](STORIES.md) |
| Plan and job-description mapping | [`ROADMAP.md`](ROADMAP.md) |
