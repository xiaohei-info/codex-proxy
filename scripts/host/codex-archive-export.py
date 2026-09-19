#!/usr/bin/env python3
"""Stream immutable completed rows; never delete them. Run under CPA's host lock."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid


def export(db_path, directory, max_bytes=16 * 1024 * 1024):
    directory = Path(directory)
    if not directory.is_dir() or not os.access(directory, os.W_OK):
        raise RuntimeError("export directory must exist and be writable")
    db = sqlite3.connect(Path(db_path).resolve().as_uri() + '?mode=rw', uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        pending = db.execute("SELECT * FROM archive_batches WHERE state='exported' ORDER BY first_request_id LIMIT 1").fetchone()
        cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        ids = json.loads(pending['request_ids_json']) if pending else []
        batch_id = pending['batch_id'] if pending else str(uuid.uuid4())
        name = f'codex-proxy-requests-{batch_id}.jsonl'
        if pending and name != pending['file_name']:
            raise RuntimeError('invalid pending file name')
        target = directory / name
        temp = directory / (name + '.part')
        digest = hashlib.sha256()
        size = 0
        selected = []
        # One row at a time: memory ceiling is largest single record, not batch size.
        rows = (db.execute('SELECT * FROM completed_requests WHERE id=?', (i,)).fetchone() for i in ids) if pending else db.execute('SELECT * FROM completed_requests WHERE created_at <= ? ORDER BY id LIMIT 500', (cutoff,))
        with open(temp, 'wb') as out:
            os.chmod(temp, 0o600)
            for row in rows:
                if row is None:
                    raise RuntimeError('pending batch lost source row')
                value = {'event_id': row['event_id'], 'created_at': row['created_at']}
                for field in ('event', 'request_headers', 'request_body', 'response_headers', 'response_body'):
                    value[field] = json.loads(row[field + '_json'])
                line = (json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
                if not pending and selected and size + len(line) > max_bytes:
                    break
                out.write(line)
                digest.update(line)
                size += len(line)
                selected.append(row['id'])
            out.flush()
            os.fsync(out.fileno())
        if not selected:
            temp.unlink()
            return {'batch': None}
        sha = digest.hexdigest()
        if pending and pending['sha256'] and sha != pending['sha256']:
            raise RuntimeError('pending receipt changed; refusing overwrite')
        os.replace(temp, target)
        # GNU find -mmin +120 rounds down, so allow a full extra minute.
        timestamp = datetime.datetime.now().timestamp() - 122 * 60
        os.utime(target, (timestamp, timestamp))
        with db:
            if not pending:
                db.execute("INSERT INTO archive_batches (batch_id,file_name,request_ids_json,row_count,first_request_id,last_request_id,cutoff,sha256,bytes,state,created_at) VALUES (?,?,?,?,?,?,?,?,?,'exported',?)", (batch_id,name,json.dumps(selected),len(selected),selected[0],selected[-1],cutoff,sha,size,cutoff))
            else:
                db.execute('UPDATE archive_batches SET sha256=?,bytes=? WHERE batch_id=?', (sha,size,batch_id))
        return {'batch': {'batch_id': batch_id, 'file_name': name, 'row_count': len(selected), 'sha256': sha}}
    finally:
        db.close()


if __name__ == '__main__':
    print(json.dumps(export(sys.argv[1], sys.argv[2])))
