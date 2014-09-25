"""
WAL-E gluster workers
"""
import subprocess

def gluster_wal_push(wal_file):
    return subprocess.call(['/usr/local/bin/gluster_util', 'wal-push', wal_file])
