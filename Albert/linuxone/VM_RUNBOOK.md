# Stage 5 on LinuxONE: copy-paste runbook

Stage 5 (Postgres + storer + dlq-storer + dashboard) has not been run on the VM yet: the session that built it had no
VM credentials. These are the exact steps. Host and key come **only from your shell environment**; never paste them
into a file in this repo (it is public).

## 0. On your laptop

```bash
export L1CC_HOST='…'          # the VM's address (ask Dhruv privately)
export L1CC_KEY=~/path/to/key # the SSH private key
ssh -i "$L1CC_KEY" linux1@"$L1CC_HOST"
```

## 1. On the VM: update the repo and create `~/fraud/.env` (once)

```bash
cd ~/fraud && git pull
uname -m                                   # must print s390x
ls ~/demo.sh ~/measure.py Datasets/sorted_test.csv Datasets/fraudTrain.csv   # all four must exist
test -f ~/fraud/.env || {
  pw=$(openssl rand -hex 16)
  sed "s/change-me/$pw/g" .env.example > ~/fraud/.env
  chmod 600 ~/fraud/.env
}
git check-ignore -v .env                   # must print the .gitignore rule: .env is never committed
```

`~/fraud/.env` must contain non-empty `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `PGHOST=postgres`,
`PGPORT=5432`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` (same value as `POSTGRES_PASSWORD`) and
`REDIS_URL=redis://redis:6379/0`, exactly the keys in `.env.example`. `deploy5.sh` refuses to start without them.

## 2. On the VM: deploy stage 5 (builds the images here, on s390x; safe to re-run)

Stages 1–4 must already be running on `fraud-net` (`redis`, `validator`, `validator2`, `scorer`, `explainer`), as
deployed per `Dhruv/README.md`.

```bash
chmod +x ~/fraud/Albert/linuxone/*.sh
~/fraud/Albert/linuxone/deploy5.sh 2>&1 | tee ~/deploy5.txt
docker ps --format '{{.Names}} ({{.Image}}) {{.Status}} {{.Ports}}'
```

Expect `postgres`, `storer`, `dlq-storer` and `dashboard` Up, and the dashboard published only on
`127.0.0.1:8080`.

## 3. On the VM: the two measured runs

```bash
{ echo '## uname -m'; uname -m; echo; echo '## docker ps'; docker ps --format '{{.Names}} ({{.Image}}) {{.Status}}'; } > ~/stage5_results.txt
for run in "1000 60000" "5000 150000"; do
  echo; echo "## demo5.sh $run"
  ~/fraud/Albert/linuxone/demo5.sh $run 2>&1
done | tee -a ~/stage5_results.txt
```

Each `demo5.sh` empties the stage 5 tables, runs the existing `~/demo.sh` unchanged (restarts the scorer, replays
fraudTest with 1% broken events), waits until stage 5's pending lists are 0 and row counts stop changing, then
`measure_db.py` prints: rows stored vs events, lost / duplicated, DLQ stored vs injected, end-to-end p50/p95/p99 from
`latency_ms` (producer `emitted_at` → Postgres commit), and recall / precision on the stream.

## 4. On your laptop: bring the results into the repo, scrubbed

```bash
cd ~/Documents/IPs/IBM-Z-Datathon-2026---Fraud-Detection
vmhost=$(ssh -i "$L1CC_KEY" linux1@"$L1CC_HOST" hostname)
{ echo '# Stage 5 on LinuxONE: raw results'; echo; echo '```';
  ssh -i "$L1CC_KEY" linux1@"$L1CC_HOST" 'cat ~/stage5_results.txt' \
  | sed -E "s/[0-9]{1,3}(\.[0-9]{1,3}){3}/<ip>/g; s/$vmhost/<vm>/g"; echo '```'; } > Albert/linuxone/RESULTS_STAGE5.md
grep -nE '([0-9]{1,3}\.){3}[0-9]{1,3}' Albert/linuxone/RESULTS_STAGE5.md   # only <ip> or version-like hits allowed
```

Then fill the "Stage 5 included" row in the root `README.md` Results table from it, commit and push (never force).

## 5. Look at the dashboard

```bash
ssh -i "$L1CC_KEY" -L 8080:127.0.0.1:8080 linux1@"$L1CC_HOST"
```

and open http://localhost:8080 on the laptop.

## Troubleshooting

- `deploy5.sh` says `.env` has no value for a key: fix `~/fraud/.env` (step 1), re-run.
- Storer logs `Postgres ... failed; retrying`: `docker logs postgres`; the storer keeps everything pending meanwhile.
- Re-deploy after a `git pull`: just re-run `deploy5.sh`; the `fraud-pg` volume keeps the data.
- Fresh database: `docker rm -f postgres && docker volume rm fraud-pg && ~/fraud/Albert/linuxone/deploy5.sh`.
