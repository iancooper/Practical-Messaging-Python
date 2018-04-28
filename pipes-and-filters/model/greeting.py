from p2pchannel.pipes_and_filters import Request


class Greeting(Request):
    def __init__(self, salutation: str):
        self.salutation = salutation

    def greet(self):
        return self.salutation