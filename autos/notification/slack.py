import requests


class IncomingWebhook:
    def __init__(self, url):
        self.url = url

    def send(self, **payload):
        return requests.post(self.url, json=payload)
