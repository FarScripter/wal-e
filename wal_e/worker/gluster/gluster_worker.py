"""
WAL-E gluster workers
"""
import subprocess

class WalGlusterUploader(object):
    def upload_to_nfs(self, segment):
        return subprocess.call(['/usr/local/bin/gluster_util', 'wal-push', segment.path]) 
