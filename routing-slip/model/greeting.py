from p2pchannel.routing_slip import RoutingSlip

enricher_routing_key = "routing.slip.enricher"
receiver_routing_key = "routing.slip.receiver"


class Greeting(RoutingSlip):
    def __init__(self, salutation: str=None, recipient: str=None):
        super().__init__()
        self.salutation = salutation
        self.recipient = recipient


