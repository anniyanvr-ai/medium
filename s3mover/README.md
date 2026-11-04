# s3mover: date-filtered, resumable MinIO → MinIO / SeaweedFS mover

s3mover copies or moves objects from MinIO to another MinIO or to SeaweedFS. It has three selection modes, can resume after any stop or crash, checks the destination after every run and shows progress on a live dashboard. Memory and request rates are capped on the mover and on both servers.

| # | Scenario | Command |
|---|---|---|
| 1 | Full move | `python -m s3mover run -c config.yaml --mode full` |
| 2 | Everything from a date (e.g. after 1 June 2025) | `python -m s3mover run -c config.yaml --mode since --from 2025-06-01` |
| 3 | Between two dates (1 June – 31 August 2025) | `python -m s3mover run -c config.yaml --mode between --from 2025-06-01 --to 2025-08-31` |

The destination is chosen in the config. `destination.type` can be `minio` or `seaweedfs` (the endpoint and keys go alongside it), and `transfer.action` can be `copy` or `move`.

## Quick start

```bash
pip install -r requirements.txt            # boto3, PyYAML, tzdata (needed for time zones on Windows)
cp config.example.yaml config.yaml         # edit endpoints, keys, buckets
python -m s3mover plan -c config.yaml      # dry run: how many objects / bytes match the filter
python -m s3mover run  -c config.yaml      # transfer + verification; dashboard on http://127.0.0.1:8765
```

* **Resume:** run the same command again. This works after Ctrl+C, a server restart, a network loss or even `kill -9`.
* **Dashboard only:** `python -m s3mover dashboard -c config.yaml`. It reads the state read-only, so you can open it any time, including from another shell.
* **Pause:** create `state/PAUSE` to pause and delete it to continue. You can also use the MinIO memory guard (see the config).
* **Check again later:** `python -m s3mover verify -c config.yaml` exits with 0 when 100% of objects are present and 2 otherwise. It lists sample keys that are missing.
* **New objects arrived since the last run:** `python -m s3mover run -c config.yaml --rescan` copies only the new matches.

Exit codes: `0` complete and verified, `2` incomplete (some objects failed or are missing), `3` interrupted (run the command again to resume), `1` config or usage error.

## Running the six scenarios on your machine (no Docker)

| Server | URL | Access key / Secret key |
|---|---|---|
| MinIO S3 API (source, and MinIO → MinIO target) | `http://localhost:9000` (console `http://localhost:9001`) | `minioadmin` / `minioadmin` |
| SeaweedFS S3 gateway (target) | `http://localhost:8333` (filer UI `http://localhost:8888`) | any value if S3 auth is off; otherwise a key from SeaweedFS `s3.json` |

The endpoints and keys are already filled in [`configs/minio-to-minio.yaml`](configs/minio-to-minio.yaml) and [`configs/minio-to-seaweedfs.yaml`](configs/minio-to-seaweedfs.yaml).

To run all six at once, set `SRC_BUCKET` at the top of `run_all_scenarios.bat` and double-click it. To run them one by one:

```bat
cd C:\Users\RLT0190\projects\s3mover
py -3 -m pip install -r requirements.txt

rem 1-3  MinIO -> MinIO (same server, new bucket per scenario)
py -3 -m s3mover run -c configs\minio-to-minio.yaml --job-name mm-full    --src-bucket rts-epcis --dest-bucket rts-epcis-full    --mode full
py -3 -m s3mover run -c configs\minio-to-minio.yaml --job-name mm-since   --src-bucket rts-epcis --dest-bucket rts-epcis-since   --mode since --from 2025-06-01
py -3 -m s3mover run -c configs\minio-to-minio.yaml --job-name mm-between --src-bucket rts-epcis --dest-bucket rts-epcis-between --mode between --from 2025-06-01 --to 2025-08-31

rem 4-6  MinIO -> SeaweedFS
py -3 -m s3mover run -c configs\minio-to-seaweedfs.yaml --job-name sw-full    --src-bucket rts-epcis --dest-bucket rts-epcis-full    --mode full
py -3 -m s3mover run -c configs\minio-to-seaweedfs.yaml --job-name sw-since   --src-bucket rts-epcis --dest-bucket rts-epcis-since   --mode since --from 2025-06-01
py -3 -m s3mover run -c configs\minio-to-seaweedfs.yaml --job-name sw-between --src-bucket rts-epcis --dest-bucket rts-epcis-between --mode between --from 2025-06-01 --to 2025-08-31
```

* **Dashboard:** http://127.0.0.1:8765 while a job runs. To browse all jobs afterwards, run `py -3 -m s3mover dashboard --state-dir state`.
* **Resume:** rerun the same line.
* **Preview:** use `plan` instead of `run` to see the counts without copying anything.
* **Move:** add `--action move` to delete from MinIO after each object's copy is verified.

## How the date filter works

* **Date used:** MinIO's `LastModified`, i.e. when the object was last written. Set `date_source: key_pattern` to use a date inside the key instead, e.g. `epcis/2025/06/01/...`.
* **`from` is inclusive:** an object written at exactly `2025-06-01 00:00:00` is included.
* **`to` as a date includes the whole day:** `2025-08-31` covers objects up to 23:59:59.999.
* **`to` as an exact timestamp:** for example `2025-08-31T18:00:00` includes objects up to that instant.
* **Time zone:** `filter.timezone` decides how dates are read. `Asia/Kolkata` makes 2025-06-01 mean 2025-05-31 18:30 UTC. The dashboard always shows the resolved UTC window.
* **One job, one filter:** each job's filter is fixed. Changing the dates, buckets, endpoints or action under the same `job.name` is refused so that two jobs never mix. Use a new `--job-name`, or `--fresh` to discard the old state.

## Why MinIO and SeaweedFS stay healthy (no OOM, no downtime)

| Risk | Control |
|---|---|
| Recursive listing of huge buckets (the usual memory spike on MinIO) | Paged `ListObjectsV2` (1,000 keys per page), split per top-level prefix. Each page is written to disk (SQLite) right away, so there is never a full listing in memory. |
| Request storms on MinIO or SeaweedFS | Two token buckets, each with a hard ceiling (`source_rps`, `dest_rps`) shared by all threads. |
| Server already struggling | Adaptive back-off: the rate is halved on 503 / SlowDown / timeouts and recovers gradually. |
| MinIO memory pressure | Optional memory guard reads MinIO's Prometheus metrics and pauses when node RSS goes over a limit. |
| Mover running out of memory | `max_inflight_mb` caps the object bytes held in memory. Objects over `multipart_threshold_mb` are streamed in `part_size_mb` parts. |
| Moving deletes data too early | Copy mode never writes to MinIO. In move mode the source is deleted only after the destination copy has been checked for size and checksum. |

Every PUT and part upload carries `Content-MD5`, so the server rejects corrupted bytes. The destination ETag is also checked against the MD5 of the bytes read.

## How "100% transferred" is guaranteed

1. **Durable listing:** each listing page is saved together with its resume cursor in one SQLite transaction. A crash causes no gaps and no duplicates.
2. **Status only after success:** an object is marked done only after the destination write succeeds. After a crash, objects that were in flight are HEAD-checked; identical ones are skipped, the rest are copied again.
3. **Failures are retried:** after `max_retries` with exponential back-off an object is marked `failed`. The next run retries failed objects automatically.
4. **Automatic verification:** after the transfer, the full destination listing is saved to disk and SQL-joined with the expected set, reporting missing objects, size mismatches and other objects at the destination. This does not depend on the destination's listing order; SeaweedFS lists differently from MinIO.
5. **Honest status:** the run reports `COMPLETE` (exit 0) only if no objects failed or are pending, every shard was scanned, and verification found 0 missing and 0 mismatched.

## Tests

```bash
pip install -r requirements-dev.txt
pytest                                                    # 65 tests: unit + integration (emulator + SeaweedFS)
S3MOVER_SEAWEED_ENDPOINT=http://localhost:8333 pytest     # use an existing SeaweedFS
python integration/run_scenarios.py --source emulator --seaweed http://127.0.0.1:8333
python integration/run_scenarios.py --source real --minio http://localhost:9000 --seaweed http://localhost:8333
```

`run_scenarios.py` runs all six scenarios (3 modes × 2 destinations). Each one is hard-killed at about 35%, resumed, then checked independently by listing the destination and comparing the MD5 of every object. Dashboard screenshots and a `REPORT.md` go to `integration/results/<run>/`. Results are in [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md).

## Large buckets: how date selection works and why a folder can match 0

* **S3 has no server-side date filter.** `ListObjectsV2` cannot ask MinIO for "objects from June to August", so every tool lists and then filters, including `rclone --max-age` and `mc find --newer-than`. s3mover lists **1,000 keys per request**, one page in memory per folder, at a capped request rate. MinIO's memory stays flat regardless of bucket size. Listing cost depends on the **number of objects** (about 1 request per 1,000 keys), not on the 2 TB of data. **Only matched objects are downloaded.**
* **Listed vs Matched:** *Listed* is every object read under the folder, including all sub-folders. *Matched* is what falls inside the window. The dashboard and `plan` output show, per folder, how many objects were **before** and **after** the window, the oldest and newest date found, and a **per-month count**. A folder with Matched 0 shows exactly where its dates are.
* **Which date is used:** by default MinIO's `LastModified`, which is the time the object was *written to MinIO*. If the files were bulk-loaded or migrated into MinIO, every object carries the load date, not the business date. If the business date is in the key, such as `EPCIS/2025/06/15/...`, use `date_source: key_pattern`.
* **Date folders are pruned automatically:** with `date_source: key_pattern` (the default `prune_date_folders: true`), s3mover walks `<folder>/<yyyy>/<m>/<d>/` level by level. Year, month and day folders outside the window are **skipped without being listed**, and only in-window day folders are read in full. Both 1- and 2-digit months and days work (`2025/11/2/`, `2025/06/01/`). Keys with no date fall back to LastModified (`key_date_fallback`). New day folders are picked up on every run. See `configs/chpd-by-folder-date.yaml`.
* **Explicit templates (optional):** you can also list the date folders yourself with `date_prefixes`. Only those day or month folders are listed, and the rest of the bucket is never touched:

```yaml
buckets:
  - source: test
    date_prefixes: ["EPCIS/{yyyy}/{mm}/{dd}/", "inbound-files/{yyyy}{mm}{dd}/"]   # {yyyy} {mm} {dd} {yy}
filter: {mode: between, from: 2025-06-01, to: 2025-08-31, date_source: key_pattern}
```

## Folder objects (keys ending in `/`)

MinIO can store data under a key that ends in `/`, such as a 1 KB object called `inbound-files-archive/`. In the MinIO console it looks like a folder. SeaweedFS can't store data under such a key: it turns the key into a directory and silently keeps 0 bytes. s3mover therefore:

* **Filters them like any other object:** by date and regex, in all three modes.
* **Renames them on SeaweedFS:** when the destination is SeaweedFS (`folder_objects: auto`), the data is written to `inbound-files-archive/.folder-object`, and the original key is recorded in the object's metadata (`s3mover-original-key`). Copies to MinIO keep the same key. Empty folder markers are copied unchanged.
* **Checks the size:** it re-reads the size from the destination and verifies against the renamed key, so a folder object can never be counted as copied while 0 bytes are stored.
* **Picks up new top-level folders:** a top-level folder created after the job was first planned is added on the next run. Use `--rescan` to list everything again.

## SeaweedFS sizing note (found during testing)

Every bucket is a SeaweedFS *collection*, and each collection needs its own writable volumes. If the volume servers run out of free slots (`-max`), PUTs stall for 30 s and then fail with `no writable volumes`. s3mover retries and backs off, but the run cannot finish until capacity is added. Before a large migration:

* give the volume servers enough `-max` slots;
* pre-grow volumes for each target bucket with `weed shell` → `volume.grow -collection <bucket> -count N`.

## Layout

```
s3mover/            engine.py (scan/transfer/verify), config.py, filters.py, state.py (SQLite),
                    ratelimit.py (adaptive token bucket), dashboard.py, cli.py
tests/unit/         config, date boundaries, filters, rate limiter, state, retry/move/integrity logic
tests/integration/  6 scenarios + graceful and kill -9 resume + move + rescan + CLI, real servers
integration/        run_scenarios.py, seaweed_cluster.sh (3 volume servers), docker-compose.yml
```
