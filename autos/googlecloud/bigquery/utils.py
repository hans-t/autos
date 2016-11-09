import time
import uuid
import random


def random_string():
    return uuid.uuid4().hex


def random_delay(mean=1):
    assert mean != 0
    delay = random.expovariate(lambd=1/mean)
    time.sleep(delay)
