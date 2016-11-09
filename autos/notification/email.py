import os
import base64
import smtplib
from email import encoders
from email.utils import COMMASPACE
from email.utils import formatdate
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(
    send_from,
    send_to,
    subject,
    text,
    paths=[],
    server="smtp.gmail.com",
    port=587,
    username='',
    password='',
    use_tls=True,
    use_html=True,
):
    """Send e-mail with or without attachment(s).

    :type send_from: string
    :param send_from: Sender e-mail address.

    :type send_to: string or list
    :param send_to: A string of recipient of e-mail address or a list of e-mail addresses.

    :type subject: string
    :param subject: E-mail subject.

    :type text: string
    :param text: E-mail text content.

    :type paths: list
    :param paths: File attachment paths.

    :type server: string
    :param server: E-mail server address.

    :type port: string
    :param port: E-mail server port.

    :type username: string
    :param username: Login username.

    :type password: string
    :param password: Login password.

    :type use_tls: bool
    :param use_tls: If true, TLS is used. Default is False.

    :type use_html: bool
    :param use_html: If true, text is interpreted as HTML (default), otherwise plaintext.
    """

    if isinstance(send_to, str):
        send_to = [send_to]

    message = MIMEMultipart()
    message['From'] = send_from
    message['To'] = COMMASPACE.join(send_to)
    message['Date'] = formatdate(localtime=True)
    message['Subject'] = subject

    subtype = 'html' if use_html else 'plain'
    message.attach(MIMEText(text, subtype, 'utf-8'))

    for path in paths:
        if not os.path.isfile(path):
            raise FileNotFoundError(path)

        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as fp:
            part.set_payload(fp.read())

        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            'attachment; filename="{}"'.format(os.path.basename(path))
        )
        message.attach(part)

    with smtplib.SMTP(server, port) as smtp:
        if use_tls:
            smtp.starttls()
        smtp.login(username,password)
        smtp.sendmail(send_from, send_to, msg=message.as_string())
