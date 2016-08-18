__all__ = ['log_exception', 'get_timed_rotating_logger']

import logging
import functools
import logging.handlers


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
        level=logging.INFO,
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

    logger = logging.getLogger(name)
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
