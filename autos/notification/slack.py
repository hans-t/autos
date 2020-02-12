import json
import urllib.request


class IncomingWebhook:
    def __init__(self, url):
        self.url = url
        self.headers = {'content-type': 'application/json'}

    def send(self, **payload):
        data = json.dumps(payload).encode('UTF-8')
        request = urllib.request.Request(
            url=self.url,
            data=data,
            headers=self.headers,
        )
        return urllib.request.urlopen(request)
