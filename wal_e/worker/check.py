from wal_e import storage
from wal_e.worker.worker_util import do_if_exist

def check_wal_backup(layout, creds, gpg_key_id, segment):
    url = '{0}/wal_{1}/{2}.lzo'.format(layout.prefix.rstrip('/'),
                            storage.CURRENT_VERSION,
                            segment.name)
    return do_if_exist(creds, url, segment.path, gpg_key_id) 

