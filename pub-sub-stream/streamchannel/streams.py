import time
from queue import Queue, Empty
from types import TracebackType

import dataclasses
from confluent_kafka import Producer, Consumer, Message
from typing import Type, Callable, Any, Optional


class Record:
    def __init__(self, id: str):
        self.id = id


def delivery_report(err: object, msg: Any):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class StreamProducer:

    def __init__(self, request_class: Type[Record], mapper_func: Callable[[Record], str],
                 bootstrap_server: str = "localhost:9092") -> None:
        """
        Wraps access to Kafka for producing a record.
        We derive topic name from the request class, which is a useful strategy for a datatype channel (i.e. one type per channel)
        :param request_class: The type of record that we want to send over this channel
        :param mapper_func: A callable that allows us to map from an object to a string
        :param bootstrap_server: The bootstrap server for the Kafka broker, defaults to "localhost:9092"
        """
        self.conf = {
            # TODO: Create a dict of config properties for Kafka
            # bootstrap.servers should be localhost:9092 (see param)
        }


        # Kafka topic
        self.topic = "Pub-Sub-Stream-" + request_class.__name__
        self.mapper_func = mapper_func

    def __enter__(self) -> 'StreamProducer':
        """ A context manager that allows us to ensure shutdown of the Kafka producer"""

        # Create a Kafka producer, from self.conf
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        """
        Ensure that the producer is flushed
        """

        # TODO: Flush the producer

        return False

    def send(self, message: Record) -> None:
        """
        We use produce to send a message and set a callback to allow asynchronous reporting for success
        What we don't show here is how we can use that delivery report to guarantee delivery (which would
        need the addition of an Outbox
        """

        # produce a message to the producer with a key of the message id and value equal to serializing the message
        # itself; register a callback for delivery_report (above), which you will receive when the message has been acked
        # by the broker


def commit_callback(kafka_error, topic_partition):
    response = {
        "kafka_error": kafka_error,
        "topic_partition": topic_partition
    }
    print("Commit info: " + str(response))


class StreamConsumer:

    def __init__(self, request_class: Type[Record], mapper_func: Callable[[str], Record],
                 bootstrap_server: str = 'localhost:9092') -> None:
        """

        :param request_class: The type of the record that we want to send over this channel
        :param mapper_func: A callable that converts a string into a Record
        :param bootstrap_server: The node in the Kafka cluster that we want to connect too
        """
        self.topic = "Pub-Sub-Stream-" + request_class.__name__
        self.mapper_func = mapper_func

        self.config = {
            # TODO: Create a dict of config properties for Kafka
            # bootstrap.servers should be localhost:9092 (see param)
            # group id should be 'pub-sub-stream'
            # enable auto commit to false - we want to commit after successfully handling the message
            # auto offset reset to earliest - what Kafka should use as the offset for a new to partition
            # on  commit set the commit_callback above to track commits
        }

    def __enter__(self) -> 'StreamConsumer':
        """ A context manager that allows us to ensure shutdown of the Kafka consumer"""

        # TODO: Create a Consumer from self.conf above

        # TODO: Subscribe the consumer to self.topic
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> bool:
        """
        A context manager that lets us clean up the consumer
        """
        self.consumer.close()
        return False

    def receive(self) -> (Record, Message):

        # listen tof a message from Kakf using the poll method
        try:
            if msg is not None:
                record = self.mapper_func(msg.value())
                return record, msg
            else:
                return None, None
        except TypeError:
            return None, None

    def commit(self, msg):
        # TODO: Commit the message, use the asynchronous flag


cancellation_token = object()


def polling_consumer(cancellation_queue: Queue, request_class: Type[Record], mapper_func: Callable[[str], Record],
                     handler_func: Callable[[Record], bool], bootstrap_server: str = 'localhost:9092') -> None:
    """
    Intended to be called from a thread, we consume messages in a loop, with a delay between reads from the queue in order
    to allow the CPU to service other requests, including the supervisor which may want to signal that we should quit
    We use a queue to signal cancellation - the cancellation token is put into the queue and a consumer checks for it
    after every loop
    :param handler_func: the function to handle messages
    :param bootstrap_server: the address of the bootstrap server in the Kafka broker§
    :param cancellation_queue: Used for inter-process communication, push a cancellation token to this to terminate
    :param request_class: What is the type of message we expect to receive on this channel
    :param mapper_func: How do we serialize messages from the wire into a python object
    """
    with StreamConsumer(request_class, mapper_func, bootstrap_server) as channel:
        while True:
            record, message = channel.receive()
            if message is not None:
                success = handler_func(record)
                channel.commit(message)
            else:
                print("Did not receive message")

            # This will block whilst it waits for a cancellation token; we don't want to wait long
            try:
                token = cancellation_queue.get(block=True, timeout=0.1)
                if token is cancellation_token:
                    print("Stop instruction received")
                    break
            except Empty:
                time.sleep(0.5)  # yield between messages
                continue
