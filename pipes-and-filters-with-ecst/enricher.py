import json
from queue import Queue
from threading import Thread
import time
from typing import Dict
from uuid import UUID

from p2pchannel.pipes_and_filters import filter, cancellation_token
from model.greeting import Greeting, EnrichedGreeting
from model.biography_reader import BiographyReader


def deserialize_message(in_message: str) -> Greeting:
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
    return json.loads(in_message, object_hook=_unserialize_instance)


def serialize_message(out_message: EnrichedGreeting) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serliaze a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(out_message, default=_serialize_instance)


def lookup_bio(name) -> str:
    db_config = {
        "host": "localhost",  # Replace with your MySQL host
        "user": "root",
        "password": "root",
        "database": "Lookup"
    }

    reader = BiographyReader(**db_config)

    biography = reader.get_biography(name)

    reader.close_connection()

    return biography


def enrich(in_request: Greeting) -> EnrichedGreeting:
    name = "Clarissa Harlow"
    bio = lookup_bio(name)
    enriched = EnrichedGreeting(in_request.salutation, name, bio)
    return enriched


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=filter, args=(cancellation_queue, Greeting, deserialize_message,
                           EnrichedGreeting, enrich, serialize_message, 'localhost'), daemon=True)

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
