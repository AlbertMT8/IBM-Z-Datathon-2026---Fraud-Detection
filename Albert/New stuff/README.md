# Contracts (source of truth for every stage)

| File | What it fixes |
| --- | --- |
| `event.schema.json` | The raw event (stage 1 → 2 → 3), v1.0.0 |
| `scored_event.schema.json` | The scored/explained event (stage 3 → 4 → 5), v1.0.0 + optional `scoring.reasons` |
| `schema.sql` | Postgres tables and views (stage 5 + dashboard) |
| `stubgen.py` | Contract-valid fake data, and `to_row()`: the one scored-event → table-row mapping (stage 5 imports it) |

`Albert/Formatting/` (an older copy without `scoring.reasons`) was deleted on Oct 18 so nothing is built against it.

## Streams

Approved names. Every arrow in the pipeline is a Redis stream; every consumer uses a consumer group and is
at-least-once (write the output, then XACK the input). Checked against the code on Oct 18 (commit `1813ce4` + stage 5).

| Stream | Written by | Read by (group) | Stream fields |
| --- | --- | --- | --- |
| `tx.raw` | stage 1 producer | stage 2 (`validators`) | `data`, `producer_id`, `seq`, `malformed` (only on injected faults) |
| `tx.valid` | stage 2 validator | stage 3 (`scorers`) | `data` (byte-identical event), `validate_ts`, `validator_id`, `producer_id`, `seq` |
| `tx.scored` | stage 3 scorer | stage 4 (`explainers`) | `data` (scored event), `event_id`, `scored_ts`, `scorer_id`, `producer_id`, `seq` |
| `tx.explained` | stage 4 explainer | stage 5 storer (`storers`) | `data` (scored event with `explanation` filled), `event_id`, `explained_ts`, `explainer_id`, `scored_ts`, `producer_id`, `seq` |
| `tx.dlq` | stages 2, 3, 4, 5 | stage 5 dlq-storer (`dlq-storers`) | `data` (raw payload), `reason`, `event_id`, `path`, `message`, `errors`, `dlq_ts`, `stage`, plus `validator_id` (stage 2) or `worker_id` (stages 3–5). Stage 2 also adds `run_id` (when the payload had one), `producer_id`, `seq` and `malformed` (the injected kind) |

DLQ `stage` values: `validation` (stage 2), `scoring` (stage 3), `explanation` (stage 4), `storage` (stage 5: a scored
event that can't be mapped, or a row Postgres rejects; reasons `MALFORMED_SCORED_EVENT`, `DB_REJECTED`).

Redis hashes for the dashboard: `metrics:validator`, `metrics:scorer`, `metrics:explainer`, `metrics:storer`.
