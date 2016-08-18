__all__ = ['merge_files', 'remove_file', 'remove_files']

import os
import glob


def merge_files(out, pattern, nbytes=1 << 30):
    filenames = sorted(glob.glob(pattern))
    if filenames:
        OVERWRITE_FLAGS = os.O_WRONLY|os.O_CREAT|os.O_TRUNC
        outfd = os.open(out, flags=OVERWRITE_FLAGS, mode=0o666)
        for filename in filenames:
            print("Merging {} to {}".format(filename, out))
            infd = os.open(filename, os.O_RDONLY)
            while os.sendfile(outfd, infd, offset=None, count=nbytes) != 0: pass
            os.remove(filename)


def remove_file(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def remove_files(*paths):
    for path in paths:
        remove_file(path)
