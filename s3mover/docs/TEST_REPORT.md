# s3mover test report

**Result:** every test passed in the cloud environment.

* **pytest:** 65 of 65 tests passed (39 unit, 26 integration).
* **Scenario runner:** all 6 scenarios passed. Each scenario was hard-killed partway through, resumed, and then checked independently: every object at the destination has the same MD5 as at the source.

| Item | Detail |
|---|---|
| Date | 2026-10-14 |
| Version | s3mover 1.0.0 |
| Host | Linux, Python 3.13 |
| Destination SeaweedFS | Real SeaweedFS 4.48 cluster: 1 master, **3 volume servers**, filer + S3 gateway, replication `001` |
| MinIO → MinIO destination | Separate S3 server process |
| Source "MinIO" | **S3 emulator (moto 5.2)**. The MinIO server binary could not be downloaded in the cloud sandbox. The emulator also lets the tests set objects' `LastModified` to real 2025 dates, so the windows are exactly *since 2025-06-01* and *2025-06-01 → 2025-08-31*. |
| Real MinIO run | `run_real_tests.bat` on the Windows workstation, against MinIO `localhost:9000` and SeaweedFS `localhost:8333`. Results go to `integration/results/real-*` (see the last section). |

## 1. pytest: 65 passed

Full output: [`test-results/pytest-output.txt`](test-results/pytest-output.txt). JUnit XML: [`test-results/pytest-junit.xml`](test-results/pytest-junit.xml).

| Area | Tests | What they prove |
|---|---|---|
| Config and date windows | 20 | `from` is inclusive. A date-only `to` covers the whole day (31 Aug 23:59:59.999 is in, 1 Sep 00:00 is out). An exact `to` timestamp is inclusive. Time zones (Asia/Kolkata, +05:30) are applied. Invalid mode/date combinations, unknown keys and same-bucket copies are rejected. Changing the filter changes the job fingerprint. |
| Filters and rate limiter | 7 | Key-pattern dates, include/exclude regexes and folder markers are handled. The token bucket enforces its rate. On throttling the rate halves, never drops below the minimum, and recovers up to the ceiling. |
| State and engine logic | 12 | A listing page and its resume cursor are saved atomically (no gaps or duplicates after a crash). Retry with back-off on `SlowDown`, throttling the correct endpoint. Gives up after `max_retries`. Identical objects are skipped. Move deletes the source **only** after a verified copy and never when the copy fails. A crash between delete and commit is recovered. Corrupted reads are detected by MD5. The memory budget blocks callers. |
| **6 core scenarios** | 6 | {full, since 2025-06-01, between Jun 1 – Aug 31} × {MinIO, SeaweedFS}. The destination key set equals the independently computed expected set, every object's MD5 matches, nothing outside the window is copied, and the source is untouched. |
| Graceful stop + resume | 6 | Same 6 combinations. The first run stops after 25 objects and the second finishes. Each object is transferred exactly once across the two runs. |
| `kill -9` + resume via CLI | 4 | The process is SIGKILLed mid-transfer and then resumed. The CLI exits 0 and every object is verified by MD5. |
| Move mode | 4 | since / between × MinIO / SeaweedFS. Objects in the window are moved, and objects outside it stay in MinIO. |
| Other integration tests | 6 | `--rescan` copies only new objects. Existing identical objects are skipped. A changed filter requires a new job. Key-pattern date source works. `plan` is a dry run. CLI `status` and `verify` work, and `verify` catches an object deleted at the destination (exit code 2). |

**Test data covers the edge cases:**

* objects at 2025-05-31 23:59:59, 2025-06-01 00:00:00, 2025-08-31 23:59:59 and 2025-09-01 00:00:00;
* 12 MB+ multipart objects;
* zero-byte objects;
* unicode and space characters in keys;
* `a-c.xml` next to `a/b.xml` (a listing-order trap);
* six-level-deep keys.

## 2. Scenario runner: 6 scenarios, kill -9, resume, independent MD5 check

Command: `python integration/run_scenarios.py --source emulator --seaweed http://127.0.0.1:8333 --small 4000`

**Data set:** 4,010 objects, 152.4 MB in total, including four 24 MB multipart objects.

**Settings:** 8 workers. Source limited to 120 requests/s and destination to 240 requests/s.

Each scenario did the following:

1. Start the CLI.
2. Hard-kill it (`SIGKILL`) at about 35%. There is no shutdown handler and no final state flush.
3. Run the same command again to resume.
4. Let the built-in verification run.
5. Check independently: list the destination, then download every object and compare its MD5 with the source.

| # | Destination | Mode | Window (UTC) | Expected | Killed at | Copied | Already present¹ | Failed | Verified (MD5) | Outside window copied | Peak RSS (mover) | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | MinIO | full | all objects | 4,010 | 1,441 | 3,977 | 33 | 0 | 4,010/4,010 (100%) | 0 | 179 MB | ✅ PASS |
| 2 | MinIO | since | ≥ 2025-06-01 00:00 | 3,008 | 1,098 | 2,904 | 104 | 0 | 3,008/3,008 (100%) | 0 | 90 MB | ✅ PASS |
| 3 | MinIO | between | 2025-06-01 → 2025-08-31 (incl.) | 2,006 | 759 | 1,999 | 7 | 0 | 2,006/2,006 (100%) | 0 | 130 MB | ✅ PASS |
| 4 | SeaweedFS | full | all objects | 4,010 | 1,465 | 3,898 | 112 | 0 | 4,010/4,010 (100%) | 0 | 186 MB | ✅ PASS |
| 5 | SeaweedFS | since | ≥ 2025-06-01 00:00 | 3,008 | 1,084 | 2,928 | 80 | 0 | 3,008/3,008 (100%) | 0 | 154 MB | ✅ PASS |
| 6 | SeaweedFS | between | 2025-06-01 → 2025-08-31 (incl.) | 2,006 | 744 | 1,954 | 52 | 0 | 2,006/2,006 (100%) | 0 | 105 MB | ✅ PASS |

¹ *Already present* objects were written to the destination just before the kill, but their status had not yet been saved. On resume, a HEAD request found them identical, so they were skipped rather than copied again. Copied + already present always equals the expected count.

**Memory:**

* **Mover:** peak RSS stayed between 90 and 186 MB (Python, boto3 and a 128 MB in-flight cap).
* **SeaweedFS:** all five `weed` processes together peaked at about 1.3 GB under load, from about 600 MB idle. No process restarted and no request failed.

Raw output: [`test-results/cloud-run/REPORT.md`](test-results/cloud-run/REPORT.md) and [`results.json`](test-results/cloud-run/results.json).

### Dashboard screenshots

Each scenario has four screenshots in `test-results/cloud-run/`: running → after kill → resumed → complete. A selection:

**MinIO → SeaweedFS, between 2025-06-01 and 2025-08-31: complete, verified 100%, with run #1 shown as killed**

![](test-results/cloud-run/seaweedfs-3-between-4-complete.png)

**MinIO → MinIO, since 2025-06-01: right after the hard kill (dashboard shows "Interrupted — resume to continue")**

![](test-results/cloud-run/minio-2-since-2-after-kill-interrupted.png)

**MinIO → SeaweedFS, full: resumed and in progress**

![](test-results/cloud-run/seaweedfs-1-full-3-resumed-in-progress.png)

**MinIO → MinIO, full: complete**

![](test-results/cloud-run/minio-1-full-4-complete.png)

**Dark mode**

![](test-results/cloud-run/dashboard-dark-mode.png)

## 3. Issue found during testing (SeaweedFS capacity)

The first SeaweedFS run used volume servers with `-max=40`. Once many test buckets existed, PUTs blocked for 30 s and then failed with `no writable volumes for collection …`. Each bucket is a separate SeaweedFS collection and needs its own volumes.

s3mover kept retrying with back-off and never marked anything complete that wasn't, but it could not finish until capacity was added. Raising `-max` fixed it.

**Production checklist:** make sure each volume server has enough free volume slots, or pre-grow volumes for each target bucket (`weed shell` → `volume.grow -collection <bucket>`), before starting a large migration. The README covers this.

## 4. Real MinIO + SeaweedFS on the Windows workstation

Run by double-clicking `run_real_tests.bat` in `C:\Users\RLT0190\projects\s3mover`. Results are in `integration\real-run.log` and `integration\results\real-*\REPORT.md`, with screenshots.

Real MinIO always sets `LastModified` to the upload time and it cannot be changed. The runner therefore uploads the data in three timed batches: before the window, inside it, and after it. The *since* and *between* windows are the instants between those batches, so they are not the literal 2025 dates.
