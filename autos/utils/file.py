__all__ = ['merge_files', 'remove_file', 'remove_files']

import os
import glob


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


def remove_files(*paths):
    for path in paths:
        remove_file(path)
