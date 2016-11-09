__all__ = [
    'SlackHookHandler',
    'EmailHandler',
    'get_logger',
    'log_exception',
    'get_timed_rotating_logger',
    'get_timed_rotating_file_handler',
    'get_slack_hook_handler',
    'get_email_handler',
    'set_root_logger',
]

import logging
import functools
import logging.handlers

import autos.notification.slack as slack
import autos.notification.email as email


DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_HANDLER_LEVEL = 'NOTSET'


class SlackHookHandler(logging.Handler):
    def __init__(self, url, username=None, channel=None):
        logging.Handler.__init__(self)
        self.url = url
        self.username = username
        self.channel = channel
        self.hook = slack.IncomingWebhook(url=url)

    def emit(self, record):
        message = self.format(record)
        self.hook.send(
            text=message,
            username=self.username,
            channel=self.channel,
        )


class EmailHandler(logging.Handler):
    def __init__(
        self,
        send_from,
        send_to,
        subject,
        username,
        password,
        **opts
    ):
        logging.Handler.__init__(self)
        self.send = functools.partial(
            email.send_email,
            send_from=send_from,
            send_to=send_to,
            subject=subject,
            username=username,
            password=password,
            **opts
        )

    def emit(self, record):
        message = self.format(record)
        self.send(text=message)


def get_logger(name=None):
    return logging.getLogger(name)


def log_exception(logger):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                logger.exception('EXCEPTION_OCCURED')
                raise
        return wrapper
    return actual_decorator


def get_timed_rotating_logger(
        name,
        filename,
        level='INFO',
        when='D',
        backup_count=7,
        log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ):
    """Create a timed rotating logger with the given name which write to filename.

    :type name: str
    :param name: Logger name.

    :type filename: str
    :param filename: Log file name.

    :rtype: logging.Logger
    :return: Logger instance.
    """

    logger = get_logger(name)
    logger.propagate = False
    logger.setLevel(level)
    formatter = logging.Formatter(log_format)
    handler = logging.handlers.TimedRotatingFileHandler(
        filename=filename,
        when=when,
        backupCount=backup_count,
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_timed_rotating_file_handler(
        filename,
        level=DEFAULT_HANDLER_LEVEL,
        when='D',
        backup_count=7,
        **opts
    ):

    handler = logging.handlers.TimedRotatingFileHandler(
        filename=filename,
        when=when,
        backupCount=backup_count,
        **opts,
    )
    handler.setLevel(level)
    return handler


def get_slack_hook_handler(
        url,
        level=DEFAULT_HANDLER_LEVEL,
        **opts
    ):

    handler = SlackHookHandler(url=url, **opts)
    handler.setLevel(level)
    return handler


def get_email_handler(
        send_from,
        username,
        password,
        level=DEFAULT_HANDLER_LEVEL,
        **opts
    ):

    handler = EmailHandler(
        send_from=send_from,
        username=username,
        password=password,
        **opts
    )
    handler.setLevel(level)
    return handler


def set_root_logger(
        *handlers,
        level='INFO',
        format=DEFAULT_LOG_FORMAT
    ):

    logging.basicConfig(
        level=level,
        format=format,
        handlers=handlers,
    )

    return get_logger()

