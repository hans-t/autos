__all__ = ['remove_file', 'remove_files', 'zip_files']

import os
import glob
import zipfile

try:
    import zlib
except:
    DEFAULT_ZIP_MODE = zipfile.ZIP_STORED
else:
    DEFAULT_ZIP_MODE = zipfile.ZIP_DEFLATED


def merge_files(dst, src_glob, nbytes=1 << 30):
    """Merge src files to dst. Only works on Linux.

    :type dst: str
    :param dst: Destination path

    :type src_glob: str
    :param src_glob: Source glob pattern.

    :type nbytes: str
    :param nbytes: Number of bytes sent per iteration.
    """

    paths = sorted(glob.iglob(src_glob))
    if paths:
        OVERWRITE_FLAGS = os.O_WRONLY|os.O_CREAT|os.O_TRUNC
        dest_fd = os.open(dst, flags=OVERWRITE_FLAGS, mode=0o666)
        for path in paths:
            src_fd = os.open(path, os.O_RDONLY)
            while os.sendfile(dest_fd, src_fd, offset=None, count=nbytes) != 0:
                pass
            os.remove(path)


def remove_file(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def remove_files(glob_pattern='', paths=()):
    if glob_pattern:
        paths = glob.iglob(glob_pattern)

    for path in paths:
        remove_file(path)


def zip_files(dest, glob_pattern='', paths=(), mode='w', compression=DEFAULT_ZIP_MODE):
    if glob_pattern:
        paths = glob.iglob(glob_pattern)

    with zipfile.ZipFile(dest, mode=mode, compression=compression) as zipper:
        for path in paths:
            filename = os.path.split(path)[1]
            zipper.write(path, arcname=filename)
