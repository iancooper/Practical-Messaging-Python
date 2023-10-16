from p2pchannel.pipes_and_filters import Request


class Greeting(Request):
    def __init__(self, salutation: str=None):
        self.salutation = salutation

    def greet(self):
        return self.salutation


class EnrichedGreeting(Greeting):
    def __init__(self, salutation:str=None, recipient: str=None, bio: str=None):
        super().__init__(salutation)
        self.recipient = recipient
        self.bio = bio

