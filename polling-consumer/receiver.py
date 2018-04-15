from queue import Queue
from threading import Thread
import time

from p2pchannel.data_type_channel import polling_consumer, cancellation_token
from model.greeting import Greeting


def map_from_message(message_body: str) -> Greeting:
    return Greeting(message_body)


def run():
    cancellation_queue = Queue()
    polling_loop = Thread(target=polling_consumer, args=(cancellation_queue, Greeting, map_from_message, 'localhost'), daemon=True)

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
