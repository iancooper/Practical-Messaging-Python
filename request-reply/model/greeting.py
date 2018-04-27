from p2pchannel.request_reply_channel import Request, Response
from uuid import UUID

# Don't use underscore properties with properties as we want to serialize them


class Greeting(Request):
    def __init__(self, salutation: str=None):
        super().__init__()
        self.salutation = salutation

    def greet(self):
        return self.salutation


class GreetingResponse(Response):
    def __init__(self, response: str=None, correlation_id: UUID=None):
        super().__init__(correlation_id=correlation_id)
        self.response = response
