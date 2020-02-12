import glob
import ftplib
import os.path


class FTP:
    def __init__(self, host, user, password, use_tls=False):
        if use_tls:
            self.client = ftplib.FTP_TLS(host, user, password)
            self.client.prot_p()
        else:
            self.client = ftplib.FTP(host, user, password)

    def upload(self, dst_dir, glob_pattern='', paths=()):
        if glob_pattern:
            paths = glob.iglob(glob_pattern)

        if paths:
            with self.client as client:
                client.cwd(dst_dir)
                for path in paths:
                    filename = os.path.split(path)[1]
                    with open(path, 'rb') as fp:
                        client.storbinary('STOR {}'.format(filename), fp)
