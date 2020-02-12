import unittest
import unittest.mock as mock

import autos.notification.email as email


class TestSendEmail(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(email, 'smtplib', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_smtplib = patcher.start()

        patcher = mock.patch.object(email, 'MIMEMultipart', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_MIMEMultipart = patcher.start()

    @mock.patch.object(email, 'MIMEText')
    def test_html_text_uses_html_type(self, mock_MIMEText):
        text = '<i>hello mars!</i>'
        email.send_email(
            send_from='you@mars.com',
            send_to='you@mars.com',
            subject='welcome to mars',
            text=text,
            use_html=True,
        )
        mock_MIMEText.assert_called_once_with(text, 'html', 'utf-8')

    @mock.patch.object(email, 'MIMEText')
    def test_plaintext_text_uses_plain_type(self, mock_MIMEText):
        text = '<i>hello mars!</i>'
        email.send_email(
            send_from='you@mars.com',
            send_to='you@mars.com',
            subject='welcome to mars',
            text=text,
            use_html=False,
        )
        mock_MIMEText.assert_called_once_with(text, 'plain', 'utf-8')

    @mock.patch.object(email, 'os')
    def test_raises_exception_when_attachment_file_not_found(self, mock_os):
        mock_os.path.isfile.return_value = False
        with self.assertRaises(FileNotFoundError):
            email.send_email(
                send_from='you@mars.com',
                send_to='you@mars.com',
                subject='welcome to mars',
                text='<i>hello mars!</i>',
                use_html=False,
                paths=['non_existent.jpg'],
            )

    def test_without_tls(self):
        email.send_email(
            send_from='you@mars.com',
            send_to='you@mars.com',
            subject='welcome to mars',
            text='<i>hello mars!</i>',
            use_html=False,
            use_tls=False,
        )
        self.assertFalse(
            self.mock_smtplib \
                .SMTP \
                .return_value \
                .__enter__ \
                .return_value \
                .starttls \
                .called
        )

    def test_with_tls(self):
        email.send_email(
            send_from='you@mars.com',
            send_to='you@mars.com',
            subject='welcome to mars',
            text='<i>hello mars!</i>',
            use_html=False,
            use_tls=True,
        )
        self.mock_smtplib \
            .SMTP \
            .return_value \
            .__enter__ \
            .return_value \
            .starttls \
            .assert_called_once_with()

    def test_accepts_single_send_to_string(self):
        send_from = 'you@mars.com'
        send_to = 'you@earth.com'
        email.send_email(
            send_from=send_from,
            send_to=send_to,
            subject='welcome to mars',
            text='<i>hello mars!</i>',
        )
        self.mock_smtplib \
            .SMTP \
            .return_value \
            .__enter__ \
            .return_value \
            .sendmail \
            .assert_called_once_with(
                send_from,
                [send_to],
                msg=self.mock_MIMEMultipart \
                        .return_value \
                        .as_string \
                        .return_value
            )




