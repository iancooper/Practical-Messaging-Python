import json

from p2pchannel.data_type_channel import Producer
from model.greeting import Greeting


def map_to_message(greeting: Greeting) -> str:
    # TODO: serialize a greeting


def run():
    with Producer(Greeting, map_to_message) as channel:
        greeting = Greeting("Hello World!")
        channel.send(greeting)
        print("Sent Message: ", json.dumps(vars(greeting)))

    input("Press [enter] to exit")


if __name__ == "__main__":
    run()




