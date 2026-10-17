# Stage 4 — Explanation container (Dhruv)

A scored event comes in from stage 3, and the same event goes out with `explanation`
filled: one plain-English sentence an analyst can act on. Follows Albert's
`scored_event.schema.json`.

## Files
| File | What it does |
| --- | --- |
| `explainer.py` | `Explainer().explain(scored)`: the logic. Template backend plus watsonx.ai backend. |
| `run.py` | **The container's entrypoint.** Scored events in on stdin, explained events out on stdout, one JSON per line. |

**No third-party libraries:** only Python's standard library. Nothing has to be compiled for s390x / big-endian, so this container is the easy one on LinuxONE.

## Run
```bash
# full stage 3 -> stage 4 chain on test data, from this folder
../.venv/bin/python ../stage3_scoring/make_events.py 1000 \
  | ../.venv/bin/python ../stage3_scoring/run.py --warm-up ../../Datasets/fraudTrain.csv \
  | ../.venv/bin/python run.py > explained.jsonl

# with the IBM AI model instead of the template
export WATSONX_APIKEY=...  WATSONX_PROJECT_ID=...      # never commit these
../.venv/bin/python run.py --backend watsonx < scored.jsonl > explained.jsonl
```

## What goes in `explanation`
| Case | status | text | model |
| --- | --- | --- | --- |
| ALLOW | `skipped` | — | backend name |
| REVIEW / BLOCK | `ok` | the sentence | `template` or the watsonx model id |
| AI call took > 800 ms | `timeout` | template sentence (safety net) | `template-fallback` |
| AI call errored | `failed` | template sentence (safety net) | `template-fallback` |
| scored event missing data | `failed` | — | backend name |

A row is **always** written: a failure here never blocks stage 5.

Example (template):
> BLOCK: $780.52 at Rodriguez, Yost and Jenkins (misc online), cardholder in Benton, WI. Why: amount $780.52 is 13.0x this card's usual $60; 2 other payments totalling $1,033 on this card in the last 24h; made at 22:00 (late night).

## What the AI is (and isn't) shown
- Shown: the decision, amount, merchant, category, cardholder city/state, and stage 3's reasons.
- Never shown: `label` (the true answer), `card_id`, `card_last4`, the score itself.
- The prompt is in `WatsonxBackend.prompt()`.

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

## Tested (on a Mac, template backend)
- 3,000 events through stage 3 → stage 4: 0 schema errors, original events untouched, ~0.01 ms per alert.
- Fake slow AI, fake broken AI, missing data, missing credentials: each gives the right status and still passes the schema.
- **The real watsonx call has NOT been tested yet:** it needs an IBM API key and project.

## Open-source / services used
- IBM watsonx.ai text generation API: https://cloud.ibm.com/apidocs/watsonx-ai
- IBM Granite models: https://www.ibm.com/granite
