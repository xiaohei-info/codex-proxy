# Codex archive host bridge

`codex-cpa-log-archive` is a host-side hook, not a second uploader. It calls
Codex Proxy's authenticated archive export endpoint, stages the returned UTF-8
JSONL beside CPA logs, invokes the existing `/usr/local/sbin/cpa-log-archive`
tar/zstd/rclone verification chain, then submits the exact
`batch_id/file_name/row_count/sha256` receipt to the authenticated commit
endpoint. Only after commit succeeds does it remove the original JSONL. Any
failure leaves SQLite rows and the export file for retry.

The Codex container must bind `archive.export_dir` (default
`/app/data/archive-export`) to the host path configured as `CODEX_EXPORT_DIR`.
The existing CPA systemd service should change only its `ExecStart` to this
bridge; the existing timer and `/usr/local/sbin/cpa-log-archive` stay intact.
Use the existing `proxy_api_key` as `CODEX_PROXY_TOKEN`.

The application intentionally has no HTTP VACUUM endpoint. Run SQLite
checkpoint/VACUUM separately under the host archive lock during a low-traffic
maintenance window after checking no bridge run is active.
