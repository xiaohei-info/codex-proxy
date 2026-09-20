# Optional request-body cold storage

Move older captured request bodies out of SQLite while retaining usage statistics in Keeper. This is an **advanced host-side integration with an existing CPA archive job**, not a turnkey cloud-backup service. The standard Docker setup does not enable it.

## What it does

1. `codex-archive-export.py` reads eligible rows (older than two hours) one at a time, writes UTF-8 JSONL and records a reusable batch receipt. It does not delete request rows.
2. `codex-cpa-log-archive` stages each batch beside CPA logs and invokes the existing `cpa-log-archive` tar/zstd/rclone pipeline under the shared lock.
3. The patched CPA job uploads directly to the configured rclone remote and verifies a SHA-256 readback. The bridge then submits the exact receipt to the authenticated commit endpoint before removing its local JSONL copy.

A failed export, upload, verification or commit retains the request rows for retry. Successful commits delete only the batch's body rows, not `integration_events`. Those bodies are no longer available through Keeper's request preview.

## Before installing

- You need Linux, Python 3 with SQLite support, Bash, curl, flock, sha256sum, tar, zstd, rclone, and an existing `/usr/local/sbin/cpa-log-archive` job compatible with [`cpa-log-archive.patch`](./cpa-log-archive.patch).
- Back up the existing script and service, review and apply the patch to a copy first, and test failure/retry behavior before enabling deletion. **The unpatched CPA job does not satisfy the bridge's verification contract.**
- The exporter opens the archive database read/write to record batch state. Give its service user access to the SQLite database, WAL/SHM files and parent directory; prepare writable export and CPA staging directories. Do not make account credentials broadly readable.
- Configure a protected environment file; use your own paths rather than the scripts' deployment-specific defaults:

| Variable | Purpose |
| --- | --- |
| `CODEX_PROXY_URL` | Proxy URL reachable from the host, e.g. `http://127.0.0.1:8080` |
| `CODEX_PROXY_TOKEN` | Existing proxy API key; keep private |
| `CODEX_ARCHIVE_DB` | Host path to `request-archive.sqlite` |
| `CODEX_EXPORT_DIR` | Prepared export directory |
| `CPA_LOG_DIR` | Existing CPA logs directory; includes the `codex-proxy` staging subdirectory |
| `CPA_ARCHIVER` | Patched CPA archive script |
| `CPA_VERIFY_REMOTE` | Direct rclone remote destination, not a FUSE mount path |
| `CODEX_EXPORT_MAX_BYTES` | Target batch size, default 128 MiB; a larger first record is exported alone |
| `CODEX_EXPORT_MAX_ROWS` | Maximum rows per batch, default 2,000 |
| `CODEX_MAX_BATCHES_PER_RUN` | Maximum batches per run, default 200 |

Install the bridge and exporter together. Point the existing service at the bridge **and load its environment file**; keep the timer schedule. Restore the original CPA script and service together if rolling back. The old HTTP bulk-export endpoint is disabled; export runs on the host, outside the proxy event loop.

## Limits and safety

- Readback verifies what the configured rclone endpoint serves. An intermediary such as AList/WebDAV is **not independent proof of final cloud-provider durability**.
- Source bodies can contain sensitive data. Restrict files and remote storage; encrypt backups where appropriate.
- Deleting SQLite rows does not automatically shrink the database file. Checkpoint/VACUUM is separate maintenance, not an HTTP endpoint or an automatic bridge action.
- [`tests/archive-staging.py`](./tests/archive-staging.py) exercises the protocol with stubs; it does not prove real remote durability.
