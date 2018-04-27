import json
import time

from p2pchannel.work_queues import Producer
from model.greeting import Greeting


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
        while True:
            with Producer(Greeting, map_to_message) as channel:
                greeting = Greeting("Hello World #" + str(loop))
                channel.send(greeting)
                print("Sent Message: ", json.dumps(vars(greeting)))
                loop += 1

                if loop % 100 == 0:
                    time.sleep(5)
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    run()




