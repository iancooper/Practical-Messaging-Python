import json

from p2pchannel.data_type_channel import Consumer
from model.greeting import Greeting


def map_from_message(message_body: str) -> Greeting:
    return Greeting(message_body)


def run():
    with Consumer(Greeting, map_from_message) as channel:
        message = channel.receive()
        if message is not None:
            print("Received message", json.dumps(vars(message)))
        else:
            print("Did not receive message")

        input("Press <CTRL+C> to exit.")


if __name__ == "__main__":
    run()
