-- =====================================================================
-- CONTRACT 3 of 3 — PostgreSQL table shape
-- IBM Z Datathon 2026 | real-time fraud scoring pipeline
--
-- Owner of stage 5. Everyone else: treat this file as read-only truth.
-- Apply with:  psql -U <user> -d fraud -f contracts/schema.sql
-- Idempotent — safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- MAIN TABLE: one row per scored transaction
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scored_transactions (
    event_id            UUID PRIMARY KEY,
    run_id              TEXT         NOT NULL DEFAULT 'default',
    source_trans_num    TEXT         NOT NULL,

    -- --- the three clocks. do not confuse them. -----------------------
    occurred_at         TIMESTAMPTZ  NOT NULL,  -- dataset time (2019-2020)
    emitted_at          TIMESTAMPTZ  NOT NULL,  -- producer emit  <- latency start
    committed_at        TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(), -- <- latency end

    -- End-to-end latency, computed by the database so no writer can forget it.
    -- If your PostgreSQL is older than 12, delete these three lines and make it
    -- a plain  latency_ms DOUBLE PRECISION  that the writer fills.
    latency_ms          DOUBLE PRECISION
                        GENERATED ALWAYS AS
                        (EXTRACT(EPOCH FROM (committed_at - emitted_at)) * 1000.0)
                        STORED,

    -- --- transaction ---------------------------------------------------
    card_id             CHAR(16)     NOT NULL,
    card_last4          CHAR(4)      NOT NULL,
    amount              NUMERIC(12,2) NOT NULL,
    merchant            TEXT         NOT NULL,
    category            TEXT         NOT NULL,
    merchant_lat        DOUBLE PRECISION,
    merchant_lon        DOUBLE PRECISION,

    -- --- cardholder ------------------------------------------------------
    holder_city         TEXT,
    holder_state        CHAR(2),
    holder_zip          CHAR(5),
    holder_lat          DOUBLE PRECISION,
    holder_lon          DOUBLE PRECISION,
    holder_city_pop     INTEGER,
    holder_job          TEXT,

    -- --- scoring ----------------------------------------------------------
    model_scores        JSONB        NOT NULL,  -- {"xgboost":0.91,"rf":0.84,...}
    ensemble_score      DOUBLE PRECISION NOT NULL
                        CHECK (ensemble_score BETWEEN 0 AND 1),
    threshold           DOUBLE PRECISION NOT NULL,
    decision            TEXT         NOT NULL
                        CHECK (decision IN ('ALLOW','REVIEW','BLOCK')),
    model_version       TEXT         NOT NULL,
    scored_at           TIMESTAMPTZ  NOT NULL,
    scoring_duration_ms DOUBLE PRECISION NOT NULL,
    scoring_reasons     JSONB,                     -- score_with_reasons() output; feeds stage 4

    -- --- explanation (nullable: stage 4 may fail, skip or lag) -------------
    explanation_text    TEXT,
    explanation_status  TEXT CHECK (explanation_status IN
                        ('ok','skipped','failed','timeout')),
    explanation_ms      DOUBLE PRECISION,
    explained_at        TIMESTAMPTZ,

    -- --- ground truth (never a model input) --------------------------------
    is_fraud            SMALLINT CHECK (is_fraud IN (0,1))
);

-- Added after the first drop. CREATE TABLE IF NOT EXISTS will not add a column
-- to a table you already created, so this catches anyone who applied the
-- earlier version.
ALTER TABLE scored_transactions ADD COLUMN IF NOT EXISTS scoring_reasons JSONB;

COMMENT ON COLUMN scored_transactions.latency_ms IS
    'End-to-end: producer emit to row commit. THE headline metric. Single definition, used identically in the dashboard, the video and interviews.';
COMMENT ON COLUMN scored_transactions.is_fraud IS
    'Ground truth from the dataset. Present so the dashboard can show live precision/recall. Must never reach the scorer.';

CREATE INDEX IF NOT EXISTS idx_st_committed   ON scored_transactions (committed_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_alerts      ON scored_transactions (committed_at DESC)
    WHERE decision <> 'ALLOW';
CREATE INDEX IF NOT EXISTS idx_st_card        ON scored_transactions (card_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_st_run         ON scored_transactions (run_id, committed_at DESC);

-- ---------------------------------------------------------------------
-- DEAD LETTER QUEUE: the demo centrepiece. Every malformed event lands
-- here with the reason attached, instead of killing the consumer.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dead_letter_events (
    dlq_id          BIGSERIAL PRIMARY KEY,
    run_id          TEXT        NOT NULL DEFAULT 'default',
    event_id        UUID,                      -- null if the id itself was unparseable
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    stage           TEXT        NOT NULL DEFAULT 'validation',
    reason_code     TEXT        NOT NULL,      -- e.g. 'schema_violation','bad_json'
    reason_detail   TEXT,                      -- the validator's message, verbatim
    raw_payload     TEXT        NOT NULL       -- exactly what arrived, for replay
);

CREATE INDEX IF NOT EXISTS idx_dlq_time   ON dead_letter_events (rejected_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_reason ON dead_letter_events (reason_code);

-- =====================================================================
-- VIEWS — the dashboard reads these, never the base tables.
-- Part 4 can build the entire UI against these four names.
-- =====================================================================

-- Live alert feed
CREATE OR REPLACE VIEW v_alert_feed AS
SELECT event_id, committed_at, card_last4, amount, merchant, category,
       holder_city, holder_state, ensemble_score, decision,
       explanation_text, explanation_status, is_fraud
FROM   scored_transactions
WHERE  decision <> 'ALLOW'
ORDER  BY committed_at DESC
LIMIT  200;

-- Sustained throughput, last 60 seconds
CREATE OR REPLACE VIEW v_throughput AS
SELECT COUNT(*)                                        AS events_last_60s,
       ROUND(COUNT(*) / 60.0, 2)                       AS events_per_sec,
       MAX(committed_at)                               AS last_event_at
FROM   scored_transactions
WHERE  committed_at > clock_timestamp() - INTERVAL '60 seconds';

-- Tail latency, last 60 seconds
CREATE OR REPLACE VIEW v_latency AS
SELECT COUNT(*)                                                       AS n,
       ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p50_ms,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p95_ms,
       ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p99_ms,
       ROUND(AVG(scoring_duration_ms)::NUMERIC, 2)                    AS avg_scoring_ms
FROM   scored_transactions
WHERE  committed_at > clock_timestamp() - INTERVAL '60 seconds';

-- Dead letter volume by reason
CREATE OR REPLACE VIEW v_dlq_summary AS
SELECT reason_code,
       COUNT(*)                                        AS total,
       COUNT(*) FILTER (WHERE rejected_at > clock_timestamp() - INTERVAL '60 seconds') AS last_60s,
       MAX(rejected_at)                                AS most_recent
FROM   dead_letter_events
GROUP  BY reason_code
ORDER  BY total DESC;

-- Live model quality against ground truth
CREATE OR REPLACE VIEW v_model_quality AS
WITH m AS (
  SELECT COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND is_fraud = 1) AS tp,
         COUNT(*) FILTER (WHERE decision <> 'ALLOW' AND is_fraud = 0) AS fp,
         COUNT(*) FILTER (WHERE decision  = 'ALLOW' AND is_fraud = 1) AS fn,
         COUNT(*)                                                     AS total
  FROM scored_transactions WHERE is_fraud IS NOT NULL
)
SELECT total, tp, fp, fn,
       ROUND(tp::NUMERIC / NULLIF(tp + fp, 0), 4) AS precision,
       ROUND(tp::NUMERIC / NULLIF(tp + fn, 0), 4) AS recall
FROM m;

-- Per-run summary, for reporting load-test numbers
CREATE OR REPLACE VIEW v_run_summary AS
SELECT run_id,
       COUNT(*)                                                       AS events,
       MIN(committed_at)                                              AS started,
       MAX(committed_at)                                              AS ended,
       ROUND((COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(committed_at) - MIN(committed_at))), 0))::NUMERIC, 2) AS sustained_eps,
       ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p50_ms,
       ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p95_ms
FROM   scored_transactions
GROUP  BY run_id;
