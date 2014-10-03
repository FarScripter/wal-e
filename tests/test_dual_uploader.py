import gevent
import pytest

from wal_e import worker


class Explosion(Exception):
    """Marker type for fault injection."""
    pass


class FakeWalSegment(object):
    def __init__(self, seg_path, explicit=False,
                 upload_explosive_blobstore=False,
                 upload_explosive_nfs=False,
                 mark_done_explosive=False):
        self.explicit = explicit
        self.path = seg_path
        self._upload_explosive_blobstore = upload_explosive_blobstore
        self._upload_explosive_nfs = upload_explosive_nfs
        self._mark_done_explosive = mark_done_explosive

        self._marked = False
        self._uploaded_blobstore = False
        self._uploaded_gluster = False

    def mark_done(self):
        if self._mark_done_explosive:
            raise self._mark_done_explosive

        self._marked = True


class FakeWalUploader(object):
    def upload_to_blobstore(self, segment):
        if segment._upload_explosive_blobstore:
            raise segment._upload_explosive_blobstore

        segment._uploaded_blobstore = True


class FakeGlusterUploader(object):
    def upload_to_nfs(self, segment):
        if segment._upload_explosive_nfs:
            raise segment._upload_explosive_nfs

        segment._uploaded_gluster = True 
        return 0


def success(seg):
    """Returns true if a segment has been successfully uploaded.

    Checks that mark_done was not called if this is an 'explicit' wal
    segment from Postgres.
    """
    if seg.explicit:
        assert seg._marked is False

    return seg._uploaded_blobstore and seg._uploaded_gluster


def failed(seg):
    """Returns true if a segment could be a failed upload.

    Or in progress, the two are not distinguished.
    """
    return seg._marked is False and (seg._uploaded_blobstore is False or seg._uploaded_gluster is False)


def indeterminate(seg):
    """Returns true as long as the segment is internally consistent.

    Checks invariants of mark_done, depending on whether the segment
    has been uploaded.  This is useful in cases with tests with
    failures and concurrent execution, and calls out the state of the
    segment in any case to the reader.
    """
    if seg._uploaded_blobstore and seg._uploaded_gluster:
        if seg.explicit:
            assert seg._marked is False
        else:
            assert seg._marked is True
    else:
        assert seg._marked is False

    return True


def prepare_multi_upload_segments():
    """Prepare a handful of fake segments for upload."""
    # The first segment is special, being explicitly passed by
    # Postgres.
    yield FakeWalSegment('0' * 8 * 3, explicit=True)

    # Additional segments are non-explicit, which means they will have
    # their metadata manipulated by wal-e rather than relying on the
    # Postgres archiver.
    for i in xrange(1, 5):
        yield FakeWalSegment(str(i) * 8 * 3, explicit=False)


def test_success_simple_upload():
    """A good case """
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)
    seg = FakeWalSegment('1'*8*3, explicit=True)
    group.start(seg)
    group.join()

    assert success(seg)


def test_success_multi_upload():
    """A good case for mutli uplaods"""
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)
    segments = list(prepare_multi_upload_segments())

    for  seg in segments:
        group.start(seg)

    group.join()

    for seg in segments:
        assert success(seg)


def do_test_fail_upload(upload_explosive_blobstore=False, upload_explosive_nfs=False):
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)
    seg = FakeWalSegment('1'*8*3, explicit=True, upload_explosive_blobstore=upload_explosive_blobstore, upload_explosive_nfs=upload_explosive_nfs)

    group.start(seg)
   
    if upload_explosive_blobstore:
        with pytest.raises(Explosion) as e:
            group.join()
        assert e.value is upload_explosive_blobstore
    else:
        with pytest.raises(Exception) as e:
            group.join()

    assert failed(seg)


def test_fail_blobstore_upload():
    exp = Explosion('fail')
    do_test_fail_upload(upload_explosive_blobstore=exp)


def test_fail_nfs_upload():
    exp = Explosion('fail')
    do_test_fail_upload(upload_explosive_nfs=exp)


def do_test_multi_explicit_fail(upload_explosive_blobstore=False, upload_explosive_nfs=False):
    """Model a failure of the explicit segment under concurrency."""
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)
    segments = list(prepare_multi_upload_segments())

    exp = Explosion('fail')
    if upload_explosive_blobstore:
        segments[0]._upload_explosive_blobstore = upload_explosive_blobstore
    else:
        segments[0]._upload_explosive_nfs = upload_explosive_nfs

    for seg in segments:
        group.start(seg)

    if upload_explosive_blobstore:
        with pytest.raises(Explosion) as e:
            group.join()
        assert e.value is upload_explosive_blobstore
    else:
        with pytest.raises(Exception) as e:
            group.join()


    assert failed(segments[0])

    for seg in segments[1:]:
        assert success(seg)


def test_multi_blobstore_explicit_fail():
    exp = Explosion('fail')
    do_test_multi_explicit_fail(upload_explosive_blobstore=exp)


def test_multi_nfs_explicit_fail():
    exp = Explosion('fail')
    do_test_multi_explicit_fail(upload_explosive_nfs=exp)


def do_test_multi_pipeline_fail(upload_explosive_blobstore=False, upload_explosive_nfs=False):
    """Model a failure of the pipelined segments under concurrency."""
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)
    segments = list(prepare_multi_upload_segments())

    fail_idx = 2
    if upload_explosive_blobstore:
        segments[fail_idx]._upload_explosive_blobstore = upload_explosive_blobstore
    else: 
        segments[fail_idx]._upload_explosive_nfs = upload_explosive_nfs

    for seg in segments:
        group.start(seg)

    if upload_explosive_blobstore:
        with pytest.raises(Explosion) as e:
            group.join()
        assert e.value is upload_explosive_blobstore
    else:
        with pytest.raises(Exception) as e:
            group.join()

    for i, seg in enumerate(segments):
        if i == fail_idx:
            assert failed(seg)
        else:
            # Given race conditions in conjunction with exceptions --
            # which will abort waiting for other greenlets to finish
            # -- one can't know very much about the final state of
            # segment.
            assert indeterminate(seg)


def test_multi_blobstore_pipeline_fail():
    exp = Explosion('fail')
    do_test_multi_pipeline_fail(upload_explosive_blobstore=exp)


def test_multi_nfs_pipeline_fail():
    exp = Explosion('fail')
    do_test_multi_pipeline_fail(upload_explosive_nfs=exp)


def test_mark_done_fault():
    """Exercise exception handling from .mark_done()"""
    dualUploader = worker.WalDualUploader(FakeWalUploader(), FakeGlusterUploader())
    group = worker.WalTransferGroup(dualUploader)

    exp = Explosion('boom')
    seg = FakeWalSegment('arbitrary', mark_done_explosive=exp)
    group.start(seg)

    with pytest.raises(Explosion) as e:
        group.join()

    assert e.value is exp



