import json
import sys
import time
from typing import Dict
from uuid import UUID

from p2pchannel.routing_slip import Producer, Step
from model.greeting import Greeting, enricher_routing_key, receiver_routing_key


def map_to_message(greeting: Greeting) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serliaze a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(greeting, default=_serialize_instance)


def run():
    try:
        loop = 1
        greeting = Greeting()
        greeting.steps[1] = Step(1, enricher_routing_key)
        greeting.steps[2] = Step(2, receiver_routing_key)
        while True:
            with Producer(greeting.steps[1].routing_key, map_to_message) as channel:
                greeting.salutation = "Hello World #" + str(loop)
                channel.send(greeting)
                print("Sent Message: ", map_to_message(greeting))
                loop += 1

                if loop % 10 == 0:
                    time.sleep(5)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    run()




