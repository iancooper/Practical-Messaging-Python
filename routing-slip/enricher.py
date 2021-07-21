import json
import time
from queue import Queue
from threading import Thread
from typing import Dict
from uuid import UUID

from model.greeting import Greeting, enricher_routing_key
from p2pchannel.routing_slip import routing_step, cancellation_token


def deserialize_message(in_message: str) -> Greeting:
    message_dict = json.loads(in_message)

    def _recursive(message_dict, base_class=Greeting):
        steps = []
        instance = base_class()
        for k, v in message_dict.items():
            if _isint(k):
                steps.append(_recursive(v, Step))
            elif isinstance(v, dict):
                setattr(instance, k, _recursive(v))
            else:
                setattr(instance, k, v)
        if steps:
            return sorted(steps, key=lambda step: step.order)
        else:
            return instance

    return _recursive(message_dict)


def serialize_message(out_message: Greeting) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serliaze a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(out_message, default=_serialize_instance)


def enrich(in_request: Greeting) -> Greeting:
    in_request.recipient = "Clarissa Harlowe"
    return in_request


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=routing_step, args=(cancellation_queue, enricher_routing_key, deserialize_message,
                                                     enrich, serialize_message, 'localhost'), daemon=True)

    polling_loop.start()

    while True:
        try:
            time.sleep(3)  # yield, delays responsiveness to keyboard interrupt though
        except KeyboardInterrupt:
            print("Shutting down consumer")
            cancellation_queue.put(cancellation_token)  # this will terminate the worker
            polling_loop.join(timeout=30)  # wait for orderly termination, if not when process ends demon will die
            break


if __name__ == "__main__":
    run()
