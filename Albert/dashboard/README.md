# ZSCORE dashboard

One self-contained page (`index.html`, polls every second) served by `server.py`: Python's standard library
`http.server` + Debian's `python3-redis` and `python3-psycopg2`. No pandas, no JavaScript build, no CDN (the chart is
inline SVG). `GET /api/state` returns everything as JSON; `GET /healthz` returns `ok`.

| Panel | Source |
| --- | --- |
| Live alert feed (latest 20 REVIEW/BLOCK, with the explanation sentence) | view `v_alert_feed` |
| Latency p50/p95/p99, last 60 s (emit → Postgres commit, `latency_ms`) | view `v_latency` |
| Latency for the whole current run (p50/p95/p99/max) and sustained rows/s | view `v_run_summary` + one query (the view has no p99) |
| Rows committed per second, last 60 s (line) | view `v_throughput` (average) + one per-second query |
| Decisions ALLOW/REVIEW/BLOCK (current run) | query |
| Alert precision / fraud recall (label used for evaluation only) | view `v_model_quality` |
| DLQ by stage and reason | query (`v_dlq_summary` has no stage) |
| Per-stage numbers | Redis hashes `metrics:validator`, `metrics:scorer`, `metrics:explainer`, `metrics:storer`, every field shown as published |

Config only via env: `REDIS_URL`, `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PORT` (8080).

## Run (on the VM; `Albert/linuxone/deploy5.sh` does this)

```bash
docker build -t fraud-dashboard:latest -f dashboard/Dockerfile .          # from Albert/, ON the VM
docker run -d --name dashboard --network fraud-net --env-file ~/fraud/.env \
  -p 127.0.0.1:8080:8080 fraud-dashboard:latest
```

It binds `0.0.0.0` inside the container but is published only on the VM's **loopback** (`127.0.0.1:8080`), so it is
not reachable on the VM's public address. Open it from your laptop through an SSH tunnel:

```bash
ssh -i "$L1CC_KEY" -L 8080:127.0.0.1:8080 linux1@"$L1CC_HOST"
```

then browse to http://localhost:8080.
