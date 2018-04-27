import json
from queue import Queue
from threading import Thread
import time
from typing import Dict
from uuid import UUID

from p2pchannel.request_reply_channel import polling_consumer, cancellation_token
from model.greeting import Greeting, GreetingResponse


def greeting_handler(greeting: Greeting) -> GreetingResponse:
    if greeting is None:
        return None

    print("Received greeting with correlation Id: {} and salutation {}".format(greeting.correlation_id, greeting.salutation))
    response = GreetingResponse("Received Greeting: {}".format(greeting.salutation), greeting.correlation_id)
    return response


def map_response(response: GreetingResponse) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serliaze a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(response, default=_serialize_instance)


def map_to_greeting(message_body: str) -> Greeting:
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


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=polling_consumer,
                          args=(cancellation_queue, Greeting, map_to_greeting, greeting_handler, map_response, 'localhost'),
                          daemon=True)

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
