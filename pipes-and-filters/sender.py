import json
import sys
import time
from typing import Dict
from uuid import UUID

from p2pchannel.pipes_and_filters import Producer
from model.greeting import Greeting


def map_to_message(greeting: Greeting) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serialize a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(greeting, default=_serialize_instance)


def run():
    try:
        loop = 1
        while True:
            with Producer(Greeting, map_to_message) as channel:
                greeting = Greeting("Hello World #" + str(loop))
                channel.send(greeting)
                print("Sent Message: ", json.dumps(vars(greeting)))
                loop += 1

                if loop % 10 == 0:
                    time.sleep(5)

    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    run()




