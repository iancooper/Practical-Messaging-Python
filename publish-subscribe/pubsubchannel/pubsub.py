from enum import Enum

import pika


class ChannelType(Enum):
    Publisher = 1
    Subscriber = 2


class pubsub:

    exchange_name = "practical-messaging-fanout"

    def __init__(self, channel_type: ChannelType, host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        Because we are trying to mirror a publish-subscribe channel we generate a random queue name
        as each consumer needs its own queue, and the publisher does not know who the consumers are
        It makes no sense for a publisher to create the queues, so we need to know if you are running as a
        publisher or consumer
        :param queue_name: The name of the queue we are using to communicate, also used as the routing key
        :param host_name: The name of the host
        """
        self._queue_name = None
        self._channel_type = channel_type
        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'pubsub':
        """
        We use a context manager as resources like connections need to be closed
        We return self as the channel is also the send/receive point in this publish-subscribe scenario
        :return: the pub-sub channel
        """
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()

        # TODO; Declare a fanout exchange, non-durable, and not auto-deleting

        if self._channel_type == ChannelType.Subscriber:
            # TODO: Declare a non-durable queue, exclusive and auto-deleting with no name
            # TODO: Get the random queue name from the result of the queue creation operation
            # TODO: Bind the queue to the exchante with the returned queue name

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        We must kill the connection, we chose to kill the channel too
        """
        self._channel.close()
        self._connection.close()

    def send(self, message: str) -> None:
        """
        We use the basic publish approach to sending over the channel. We don't need a routing key as the
        fanout exchange sends our message to every queue that we have on the exchange
        :param message: The message we want to send
        """

        # TODO: Publish the message with an empty routing key

    def receive(self) -> str:
        """
        We just use a basic get on the channel to retrieve the message, and return the body if it
        exists
        :return: The message or None if we could not read from the queue
        """
        method_frame, header_frame, body = self._channel.basic_get(queue=self._queue_name, no_ack=True)
        if method_frame is not None:
            return body
        else:
            return None



