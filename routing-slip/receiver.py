import json
from queue import Queue
from threading import Thread
import time
from typing import Dict
from uuid import UUID

from p2pchannel.routing_slip import polling_consumer, cancellation_token
from model.greeting import Greeting, receiver_routing_key


def map_from_message(message_body: str) -> Greeting:
    def _unserialize_instance(d: Dict) -> object:
        for key, value in d.items():
            if isinstance(value, str):  # We need to check if the string on the wire is actually a UUID, by conversion
                try:
                    guid = UUID(value)
                    value = guid
                except ValueError:
                    pass
            setattr(greeting, key, value)
        return greeting

    greeting = Greeting()
    return json.loads(message_body, object_hook=_unserialize_instance)


def handle_greeting(greeting: Greeting) -> Greeting:
    print(greeting.salutation + " " + greeting.recipient)
    return greeting


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=polling_consumer, args=(cancellation_queue, receiver_routing_key,
                            handle_greeting, map_from_message, 'localhost'), daemon=True)

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
