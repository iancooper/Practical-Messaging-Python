from datetime import datetime
import json
from typing import Dict
from uuid import UUID, uuid4

from p2pchannel.request_reply_channel import Producer
from model.greeting import Greeting, GreetingResponse


def map_to_message(greeting: Greeting) -> str:
    def _serialize_instance(obj: object) -> Dict:
        d = {}
        d.update(vars(obj))
        for key, value in d.items():
            if isinstance(value, UUID):  # json does not know how to serliaze a UUID, so convince it is a string instead
                d[key] = str(value)
        return d

    return json.dumps(greeting, default=_serialize_instance)


def map_to_greeting_response(message_body: str) -> GreetingResponse:
    def _unserialize_instance(d: Dict) -> object:
        for key, value in d.items():
            if isinstance(value, str):  # We need to check if the string on the wire is actually a UUID, by conversion
                try:
                    guid = UUID(value)
                    value = guid
                except ValueError:
                    pass
            setattr(response, key, value)
        return response

    response = GreetingResponse()
    return json.loads(message_body, object_hook=_unserialize_instance)


def run():
    with Producer(Greeting, map_to_message, map_to_greeting_response) as channel:
        greeting = Greeting("Hello World!")

        response = channel.call(greeting, 5000)
        print("Sent Message: {}".format(greeting.salutation))
        if response is not None:
            print("Response: {} Correlation Id {} at {}".format(response.response, response.correlation_id, datetime.utcnow()))
        else:
            print("Did not receive a response")


    input("Press [enter] to exit")


if __name__ == "__main__":
    run()




