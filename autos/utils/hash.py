from hashlib import md5
from hashlib import sha1


def hash_str(string, hash_type='md5'):
    byte = string.encode('UTF-8')
    if hash_type == 'md5':
        hasher = md5
    elif hash_type == 'sha1':
        hasher = sha1
    else:
        raise ValueError('Unknown hash type: {}'.format(hash_type))
    return hasher(byte).hexdigest()
