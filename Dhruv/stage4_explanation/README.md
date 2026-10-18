# Stage 4 — Explanation container (Dhruv)

A scored event comes in from stage 3, and the same event goes out with `explanation`
filled: one plain-English sentence an analyst can act on. Follows Albert's
`scored_event.schema.json`.

In the pipeline: **Redis `tx.scored` → `stream_worker.py` → Redis `tx.explained`** (stage 5 reads `tx.explained`).

## Files
| File | What it does |
| --- | --- |
| `explainer.py` | `Explainer().explain(scored)`: the logic. Three backends: template, watsonx.ai, local Granite (llama.cpp). |
| `stream_worker.py` | **The container's entrypoint.** Reads Redis `tx.scored`, fills `explanation`, writes `tx.explained`. |
| `run.py` | Same over stdin/stdout (one JSON per line), for quick local tests without Redis. |
| `Dockerfile` | The stage 4 container (python:3.12-slim + redis). Build on the LinuxONE VM. |
| `Dockerfile.llama` | Optional: the local Granite model server (llama.cpp built from source). Not verified yet. |

**Only one third-party library, `redis`, which is pure Python.** Nothing has to be compiled for s390x / big-endian, so this container is the easy one on LinuxONE.

## How the worker keeps the pipeline fast
- ALLOW events (~99.6%) and template sentences are done instantly and passed straight on.
- Alerts that need the AI (`watsonx` / `local`) go to background threads (`--ai-threads`, default 2), so a 2–5 s
  AI call never holds up the payments behind it.
- If more than `--max-ai-waiting` (default 8) alerts are already waiting for the AI, new alerts get the template
  sentence immediately (`model: "template"`, counted as `ai_busy_used_template`) instead of piling up.
- Each input is XACKed only after its result is written (at-least-once, like stages 2 and 3).
- Stage 4 keeps no memory between events, so you can run several copies (each with its own `--consumer`).
  A restarted worker takes over entries a crashed one never finished.
- Live numbers go to the Redis hash `metrics:explainer` (explained, ok/skipped/failed/timeout, ai_waiting, alert_p50_ms, alert_p95_ms).

## Docker on LinuxONE (build on the VM itself)
```bash
# from the repo root, on the VM, on the same fraud-net network as Redis
docker build -t fraud-explainer:latest Dhruv/stage4_explanation
docker run -d --name explainer --network fraud-net fraud-explainer:latest                    # template
docker run -d --name explainer --network fraud-net -e BACKEND=local \
    -e LLAMA_URL=http://llm:8080 -e TIMEOUT_MS=10000 fraud-explainer:latest                  # local Granite
docker run -d --name explainer --network fraud-net -e BACKEND=watsonx \
    -e WATSONX_APIKEY -e WATSONX_PROJECT_ID fraud-explainer:latest                           # watsonx (keys from your shell)
```
For `local`, the model server runs as its own container (`Dockerfile.llama`, pinned to llama.cpp b11669) with a
big-endian Granite file mounted at `/models`. **Not verified yet:** no Docker or LinuxONE machine was available.

## Run
```bash
# full stage 3 -> stage 4 chain on test data, from this folder
../.venv/bin/python ../stage3_scoring/make_events.py 1000 \
  | ../.venv/bin/python ../stage3_scoring/run.py --warm-up ../../Datasets/fraudTrain.csv \
  | ../.venv/bin/python run.py > explained.jsonl

# with IBM's hosted AI (watsonx.ai)
export WATSONX_APIKEY=...  WATSONX_PROJECT_ID=...      # never commit these
../.venv/bin/python run.py --backend watsonx < scored.jsonl > explained.jsonl

# with a Granite model we run ourselves (llama-server must already be running, see below)
../.venv/bin/python run.py --backend local --timeout-ms 5000 < scored.jsonl > explained.jsonl
```

## Which backend?
| Backend | Generative AI? | Needs | Speed | Use it for |
| --- | --- | --- | --- | --- |
| `template` (default) | No, fills a fixed sentence | nothing | ~0.01 ms | load tests; the safety net behind the other two |
| `watsonx` | Yes, Granite on IBM Cloud | API key + project ID | network round trip | if the team gets watsonx access |
| `local` | Yes, Granite on our own machine | `llama-server` running | measured on the Mac: ~2.3 s per alert with its graphics chip, ~5 s processor only | backup while watsonx is hard to get; "AI runs on LinuxONE itself" |

## What goes in `explanation`
| Case | status | text | model |
| --- | --- | --- | --- |
| ALLOW | `skipped` | — | backend name |
| REVIEW / BLOCK | `ok` | the sentence | `template`, the watsonx model id, or the local model file name |
| AI call took longer than `--timeout-ms` (default 800) | `timeout` | template sentence (safety net) | `template-fallback` |
| AI call errored | `failed` | template sentence (safety net) | `template-fallback` |
| AI sentence used a number that isn't in the facts (e.g. "130x" for "13.0x") | `failed` | template sentence (safety net) | `template-fallback` |
| scored event missing data | `failed` | — | backend name |

A row is **always** written: a failure here never blocks stage 5.

Example (template):
> BLOCK: $780.52 at Rodriguez, Yost and Jenkins (misc online), cardholder in Benton, WI. Why: amount $780.52 is 13.0x this card's usual $60; 2 other payments totalling $1,033 on this card in the last 24h; made at 22:00 (late night).

## What the AI is (and isn't) shown
- Shown: the decision, amount, merchant, category, cardholder city/state, and stage 3's reasons.
- Never shown: `label` (the true answer), `card_id`, `card_last4`, the score itself.
- The prompt is in `build_prompt()`. Both AI backends get the exact same prompt: rules, one worked example, then the facts.
- **We don't trust the AI blindly.** `check_numbers()` rejects any sentence containing a number that isn't in the facts it was given.
  A small model's typical mistake is getting numbers wrong ("130x" for "13.0x"), and this check catches it.
  It can't catch wrong wording (e.g. calling a $1,033 *total* "$1,033 transactions").

## Local Granite backend (llama.cpp)
[llama.cpp](https://github.com/ggml-org/llama.cpp) is a free, open-source program that runs AI models on an ordinary processor.
Its `llama-server` tool opens a small web address that stage 4 sends prompts to.

**On LinuxONE (s390x):** follow llama.cpp's [`docs/build-s390x.md`](https://github.com/ggml-org/llama.cpp/blob/master/docs/build-s390x.md). The key points:
1. Build llama.cpp from source on the VM (`cmake -B build && cmake --build build --config Release`).
2. **The model file must be big-endian.** A normal model file won't load. Either:
   - convert IBM's model yourself (the docs' recommended route):
     `python convert_hf_to_gguf.py <downloaded ibm-granite/granite-3.3-2b-instruct folder> --outtype f16 --bigendian --outfile granite-3.3-2b-instruct-be.f16.gguf`, or
   - use a ready-made big-endian file (usually named `...-be...gguf`).
3. Start it: `./build/bin/llama-server -m granite-3.3-2b-instruct-be.f16.gguf --port 8080`
4. Run stage 4 with `--backend local --timeout-ms 5000`.

**On a Mac (what we tested with).** Download a normal (little-endian) build and model into `local_llm/` at the repo root (git ignores it):
- `llama-b11669-bin-macos-arm64.tar.gz` (12.2 MB) from https://github.com/ggml-org/llama.cpp/releases/tag/b11669, the build behind stable v0.7.0.
  sha256 `dbed55493d72bc1ba2babc8770d669c2289b5bc4ee9fb2a4dfc158d45b7d13fd`
- `granite-3.3-2b-instruct-Q4_K_M.gguf` (1.55 GB) from https://huggingface.co/ibm-granite/granite-3.3-2b-instruct-GGUF (Apache-2.0).
  sha256 `ac71e9e32c0bea919b409c5918f69ca74339854b0319c5065e4e9fb6d95c4852`
```bash
cd local_llm
llama.cpp-b11669/llama-b11669/llama-server -m models/granite-3.3-2b-instruct-Q4_K_M.gguf --host 127.0.0.1 --port 8080 -c 2048 -np 1
```

**Measured on the Mac (Apple A18 Pro, Oct 17), 17 real alerts, Granite 3.3 2B Q4_K_M:**
| Setup | Per sentence (median / worst) | With the schema's 800 ms limit |
| --- | --- | --- |
| Mac graphics chip (Metal) | 2.3 s / 2.8 s | all 17 time out → template sentence |
| Processor only (`-ngl 0`), closest stand-in for LinuxONE | 5.0 s / 7.4 s | all 17 time out → template sentence |

LinuxONE will differ (different processor, no graphics chip), so **measure it there**. Expect seconds, not milliseconds.

Example sentences (all 17 passed the number check):
> $780.52 large purchase at 22:00, 13.0x this card's usual $60, alongside two other recent $1,033 transactions.
> $1,077.69 large online purchase at 22:00, 19.2x this card's usual $56, 1,119 minutes since last payment; BLOCKED.

**Two team decisions this creates:**
1. **Timeout.** At 800 ms the local model never answers in time. For `local`, run with `--timeout-ms 10000`.
   It only runs on alerts (~0.4% of payments) and never holds up scoring, but Albert's contract says 800 ms, so agree it with him.
2. **Keeping up.** One sentence takes 2–5 s, so the model can only handle about one alert every few seconds. At high replay rates
   alerts would pile up. Use `template` for load tests and `local` for the live demo at a calm replay rate.

Settings: `LLAMA_URL` (default `http://127.0.0.1:8080`), `LLAMA_MODEL_NAME` (optional; otherwise the server's model file name is used).

## Why watsonx.ai and not watsonx Assistant
The original team plan named watsonx Assistant. We chose **watsonx.ai text generation**
instead (team decision, Oct 17):
- watsonx Assistant is for building chatbots, i.e. conversations a person has with a bot.
- watsonx.ai takes a prompt and returns text, which is exactly what this stage does:
  facts in, one sentence out.
- It's still IBM's generative AI, so it still covers the "Predictive + Generative AI" lab
  (stage 3 predicts, stage 4 generates).

## watsonx settings (environment variables)
| Variable | Default |
| --- | --- |
| `WATSONX_APIKEY` | required |
| `WATSONX_PROJECT_ID` | required |
| `WATSONX_URL` | `https://us-south.ml.cloud.ibm.com` |
| `WATSONX_MODEL_ID` | `ibm/granite-3-8b-instruct`. Check it's still offered in your region. |

## Tested (on a Mac)
- 3,000 events through stage 3 → stage 4: 0 schema errors, original events untouched, ~0.01 ms per alert.
- Fake slow AI, fake broken AI, missing data, missing credentials: each gives the right status and still passes the schema.
- Local backend against a **fake** `llama-server` (a stand-in that answers like the real one): AI sentences come through and are cleaned up,
  a 2 s reply hits the 800 ms limit and falls back to the template (status `timeout`), the same reply passes with `--timeout-ms 5000`,
  and no running server stops it at startup with a clear message. No card ID, last 4 digits or fraud label was sent in any prompt.
- **Real Granite 3.3 2B through llama-server on the Mac:** 17/17 alerts got AI sentences, 0 schema errors. The number check works:
  a first, looser prompt produced "130x" for "13.0x", which is what led to the worked-example prompt and the check.
- **Pipeline test with Redis (fake Redis server, Mac):** 20,000 events from Albert's real producer → validator → stage 3 →
  this worker: every event explained exactly once, 0 schema errors, original event text unchanged. With stage 3 and
  stage 4 both `kill -9`'d mid-stream and restarted: 0 lost, 2 written twice (stage 5 drops repeats by `event_id`).
  The restarted worker also rode out a dropped Redis connection.
- **Not tested yet:** the real watsonx call (needs an IBM API key), the AI backends through the Redis worker, and anything on LinuxONE or in Docker.

## Open-source / services used
- IBM watsonx.ai text generation API: https://cloud.ibm.com/apidocs/watsonx-ai
- IBM Granite models: https://www.ibm.com/granite · on Hugging Face: https://huggingface.co/ibm-granite
- llama.cpp (MIT licence): https://github.com/ggml-org/llama.cpp
- redis-py: https://github.com/redis/redis-py
