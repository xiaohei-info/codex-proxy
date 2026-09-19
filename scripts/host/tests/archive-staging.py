#!/usr/bin/env python3
"""Linux-only isolated contract test. Argument: COPY of installed CPA archiver.
No network/upload credentials: rclone and the authenticated HTTP commit are stubs.
"""
import fcntl
import hashlib
import http.server
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading

ARTIFACTS = Path(os.environ.get('ARCHIVE_ARTIFACTS', '/tmp'))
assert sys.platform.startswith('linux'), 'Run in Linux, not macOS'


def run(args, **kwargs):
    return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)


def executable(path, text):
    path.write_text(text)
    path.chmod(0o700)


with tempfile.TemporaryDirectory(prefix='archive-contract-') as temporary:
    root = Path(temporary)
    original = root / 'cpa'
    shutil.copyfile(sys.argv[1], original)
    for mode in ('--dry-run', ''):
        result = run(['patch'] + ([mode] if mode else []) + [str(original), str(ARTIFACTS / 'cpa-log-archive.patch')])
        assert result.returncode == 0, result.stdout
    print('PASS patch dry-run/apply on installed-script copy')
    patched = original.read_text()
    bins = root / 'bin'
    bins.mkdir()
    executable(bins / 'findmnt', '#!/bin/sh\necho fuse.rclone\n')
    # No sudo is executed in staging. All selected files belong to this test.
    executable(bins / 'sudo', '#!/bin/sh\n[ "$1" = -n ] && shift\nexec "$@"\n')
    executable(bins / 'rclone', '''#!/usr/bin/env python3
import os,sys,shutil
from pathlib import Path
mode=os.environ['TEST_MODE']
if sys.argv[1]=='copyto':
 if mode=='upload_failure': sys.exit(17)
 shutil.copyfile(sys.argv[2],sys.argv[3]);sys.exit(0)
if sys.argv[1]=='cat':
 if mode=='checksum_failure': sys.stdout.buffer.write(b'corrupt');sys.exit(0)
 sys.stdout.buffer.write(Path(sys.argv[2]).read_bytes());sys.exit(0)
sys.exit(18)
''')

    for failure in ('success', 'upload_failure', 'checksum_failure', 'commit_failure', 'lock'):
        case = root / failure
        case.mkdir()
        for directory in ('export', 'logs/codex-proxy', 'remote'):
            (case / directory).mkdir(parents=True)
        dbpath = case / 'archive.sqlite'
        db = sqlite3.connect(dbpath)
        db.executescript('''
CREATE TABLE completed_requests(id INTEGER PRIMARY KEY,event_id TEXT,event_json TEXT,request_headers_json TEXT,request_body_json TEXT,response_headers_json TEXT,response_body_json TEXT,created_at TEXT);
CREATE TABLE integration_events(seq INTEGER PRIMARY KEY,event_json TEXT);
CREATE TABLE archive_batches(batch_id TEXT PRIMARY KEY,file_name TEXT,request_ids_json TEXT,row_count INTEGER,first_request_id INTEGER,last_request_id INTEGER,cutoff TEXT,sha256 TEXT,bytes INTEGER,state TEXT,created_at TEXT,committed_at TEXT);
''')
        for i in range(1, 4):
            db.execute('INSERT INTO completed_requests VALUES(?,?,?,?,?,?,?,?)', (i, f'e{i}', json.dumps({'request_id': f'r{i}'}), '{}', json.dumps({'prompt': '秘密', 'i': i}), '{}', json.dumps({'answer': i}), '2020-01-01T00:00:00.000Z' if i < 3 else '2999-01-01T00:00:00.000Z'))
            db.execute('INSERT INTO integration_events VALUES(?,?)', (i, '{}'))
        db.commit()
        db.close()
        lock = str(case / 'shared.lock')
        cpa = patched.replace('/tmp/cpa-log-archive.lock', lock)
        cpa = cpa.replace('/home/ubuntu/services/ai-gateway/CLIProxyAPI/logs', str(case / 'logs'))
        cpa = cpa.replace('/mnt/rclone/quark/中转站/oracle-backup', str(case / 'remote'))
        executable(case / 'cpa', cpa)
        executable(case / 'bridge', (ARTIFACTS / 'codex-cpa-log-archive').read_text().replace('/tmp/cpa-log-archive.lock', lock))
        shutil.copyfile(ARTIFACTS / 'codex-archive-export.py', case / 'codex-archive-export.py')
        state = {'mode': failure, 'calls': 0}

        class Commit(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                state['calls'] += 1
                assert self.path == '/admin/integration/keeper/archive/commit'
                receipt = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if state['mode'] == 'commit_failure':
                    self.send_response(503)
                    self.end_headers()
                    return
                with sqlite3.connect(dbpath) as connection:
                    connection.row_factory = sqlite3.Row
                    batch = connection.execute('SELECT * FROM archive_batches WHERE batch_id=?', (receipt['batch_id'],)).fetchone()
                    assert batch and all(receipt[k] == batch[k] for k in ('file_name', 'row_count', 'sha256'))
                    # Verify the exact staged JSONL is present in a real tar.zst,
                    # uploaded by the patched CPA script to the fake remote.
                    found = False
                    for archive in (case / 'remote').glob('*.tar.zst'):
                        payload = subprocess.check_output(['tar', '--zstd', '-xOf', str(archive), 'codex-proxy/' + batch['file_name']])
                        if hashlib.sha256(payload).hexdigest() == batch['sha256']:
                            found = True
                    assert found, 'commit without verified archive payload'
                    ids = json.loads(batch['request_ids_json'])
                    connection.executemany('DELETE FROM completed_requests WHERE id=?', [(i,) for i in ids])
                    connection.execute("UPDATE archive_batches SET state='committed' WHERE batch_id=?", (batch['batch_id'],))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status":"committed"}')

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Commit)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        env = dict(os.environ, PATH=str(bins) + ':' + os.environ['PATH'], CODEX_PROXY_URL=f'http://127.0.0.1:{server.server_port}', CODEX_PROXY_TOKEN='staging-only', CODEX_ARCHIVE_DB=str(dbpath), CODEX_EXPORT_DIR=str(case / 'export'), CPA_LOG_DIR=str(case / 'logs'), CPA_ARCHIVER=str(case / 'cpa'), CPA_VERIFY_REMOTE=str(case / 'remote'), TEST_MODE=failure)
        held = open(lock, 'w') if failure == 'lock' else None
        if held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        first = run(['bash', str(case / 'bridge')], env=env, timeout=30)
        with sqlite3.connect(dbpath) as connection:
            ids = [r[0] for r in connection.execute('SELECT id FROM completed_requests ORDER BY id')]
            assert ids == ([3] if failure == 'success' else [1, 2, 3]), (failure, first.stdout, ids)
            pending = connection.execute('SELECT batch_id FROM archive_batches').fetchone()
        assert first.returncode == (0 if failure in ('success', 'lock') else first.returncode), first.stdout
        if failure not in ('success', 'lock'):
            assert first.returncode != 0, first.stdout
        if failure in ('upload_failure', 'checksum_failure', 'lock'):
            assert state['calls'] == 0, 'commit must not be attempted'
        print(f'PASS {failure}: exit={first.returncode}; source IDs={ids}')
        if held:
            held.close()
        if failure != 'success':
            state['mode'] = 'success'
            env['TEST_MODE'] = 'success'
            retry = run(['bash', str(case / 'bridge')], env=env, timeout=30)
            assert retry.returncode == 0, retry.stdout
            with sqlite3.connect(dbpath) as connection:
                assert connection.execute('SELECT id FROM completed_requests').fetchall() == [(3,)]
                assert connection.execute('SELECT count(*) FROM integration_events').fetchone()[0] == 3
                if pending:
                    assert connection.execute('SELECT batch_id FROM archive_batches').fetchall() == [pending]
            print(f'PASS {failure} retry: exit=0; same batch committed; only IDs 1,2 deleted')
        empty = run(['bash', str(case / 'bridge')], env=env, timeout=30)
        assert empty.returncode == 0, empty.stdout
        assert not list((case / 'export').glob('*.jsonl'))
        server.shutdown()
        server.server_close()

    # A row larger than the batch target must not be silently skipped forever.
    spec = importlib.util.spec_from_file_location('exporter', ARTIFACTS / 'codex-archive-export.py')
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    with sqlite3.connect(dbpath) as connection:
        connection.execute("UPDATE completed_requests SET created_at='2020-01-01T00:00:00.000Z', request_body_json=? WHERE id=3", (json.dumps('x' * (17 * 1024 * 1024)),))
    result = exporter.export(str(dbpath), str(case / 'export'))
    assert result['batch']['row_count'] == 1
    assert (case / 'export' / result['batch']['file_name']).stat().st_size > 16 * 1024 * 1024
    with sqlite3.connect(dbpath) as connection:
        assert connection.execute('SELECT id FROM completed_requests').fetchall() == [(3,)]
    print('PASS oversized first row: exported intact alone; source retained; 16MiB is target, not hard cap')
print('PASS all isolated Linux staging assertions')
