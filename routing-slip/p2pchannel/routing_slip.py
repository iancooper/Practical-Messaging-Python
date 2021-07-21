import json
import pika
from queue import Empty, Queue
import time
from typing import Callable, Dict, Type

exchange_name = "practical-messaging-work-queue"
invalid_message_exchange_name = "practical-messaging-invalid"


class Request:
    pass

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"


class Step:
    def __init__(self, order: int=None, routing_key: str=None):
        self.order = order
        self.completed = False
        self.routing_key = routing_key

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"


class RoutingSlip(Request):
    def __init__(self):
        self.steps = {}  # type:  Dict[int, Step]
        self.current_step = 0


class Producer:
    def __init__(self, destination_routing_key: str, mapper_func: Callable[[Request], str], host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        We use the request class to get the name of the routing key. We could use any approach to agree a routing key
        between producer and consumer, but using the name of the type that the queue is of makes a certain sense
        We name the queue after the routing key because we are building a point-to-point channel with a single consudmer
        We split the producer and consumer because one serializes and the other deserializes and we do not wish to make
        a single DataType channel class with both dependencies
        :param destination_routing_key: the first step in the routing slip sequence
        :param mapper_func: The function that maps
        :param host_name: The name of the host
        """
        self._routing_key = destination_routing_key
        self._queue_name = self._routing_key
        self._mapper_func = mapper_func
        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'Producer':
        """
        We use a context manager as resources like connections need to be closed
        We return self as the channel is also the send/receive point in this point-to-point scenario

        We establish an exchange to use for invalid messages (RMQ confusingly calls this dead-letter) and a routing key
        to use when we reject messages to this queue, so that we can create a subscribing queue on the exchange that picks
        up the invalid messages.
        :return: the point-to-point channel
        """
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, auto_delete=False)

        invalid_routing_key = 'invalid.' + self._routing_key
        invalid_queue_name = invalid_routing_key

        args = {'x-dead-letter-exchange': invalid_message_exchange_name, 'x-dead-letter-routing-key': invalid_routing_key}

        # If we are going to use persistent messages it makes sense to have a durable queue definition
        self._channel.queue_declare(queue=self._queue_name, durable=True, exclusive=False, auto_delete=False, arguments=args)
        self._channel.queue_bind(exchange=exchange_name, routing_key=self._routing_key, queue=self._queue_name)

        self._channel.exchange_declare(exchange=invalid_message_exchange_name, exchange_type='direct', durable=True, auto_delete=False)
        self._channel.queue_declare(queue=invalid_queue_name, durable=True, exclusive=False, auto_delete=False)
        self._channel.queue_bind(exchange=invalid_message_exchange_name, routing_key=invalid_routing_key, queue=invalid_queue_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        We must kill the connection, we chose to kill the channel too
        """
        self._channel.close()
        self._connection.close()

    def send(self, message: Request) -> None:
        """
        We use the basic publish approach to sending over the channel. The routing key is the type
        :param message: The message we want to send
        """
        message_body = self._mapper_func(message)
        self._channel.basic_publish(exchange=exchange_name, routing_key=self._routing_key, body=message_body,
                                    properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
                                    )


class Consumer:
    def __init__(self, source_routing_key: str, mapper_func: Callable[[str], Request], host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        :param source_routing_key: We are receiving messages from this routing key
        :param mapper_func: How do we serialize messages back into a type
        :param host_name: where is the rmq node we are connecting to
        """
        self._routing_key = source_routing_key
        self._queue_name = self._routing_key
        self._mapper_func = mapper_func

        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'Consumer':
        """
        We use a context manager as resources like connections need to be closed
        We return self as the channel is also the send/receive point in this point-to-point scenario

        We establish an exchange to use for invalid messages (RMQ confusingly calls this dead-letter) and a routing key
        to use when we reject messages to this queue, so that we can create a subscribing queue on the exchange that picks
        up the invalid messages.
        :return: the point-to-point channel
        """
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True, auto_delete=False)

        invalid_routing_key = 'invalid.' + self._routing_key
        invalid_queue_name = invalid_routing_key

        args = {'x-dead-letter-exchange': invalid_message_exchange_name, 'x-dead-letter-routing-key': invalid_routing_key}

        # We want a durable queue definition, so that someone queues message to recipients, even if they are not there
        self._channel.queue_declare(queue=self._queue_name, durable=True, exclusive=False, auto_delete=False, arguments=args)
        self._channel.queue_bind(exchange=exchange_name, routing_key=self._routing_key, queue=self._queue_name)

        self._channel.exchange_declare(exchange=invalid_message_exchange_name, exchange_type='direct', durable=True, auto_delete=False)
        self._channel.queue_declare(queue=invalid_queue_name, durable=True, exclusive=False, auto_delete=False)

        # We set this to enforce fairness amongst consumers
        self._channel.basic_qos(prefetch_count=1)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        We must kill the connection, we chose to kill the channel too
        """
        self._channel.close()
        self._connection.close()

    def receive(self) -> Request:
        """
        We just use a basic get on the channel to retrieve the message,
        But what we get back is a byte array really, and we need to convert that to a str so that we can use it
        We ignored this in prior exercises, because 'it just worked' but now we care about it
        :return: The message or None if we could not read from the queue
        """
        method_frame, header_frame, body = self._channel.basic_get(queue=self._queue_name, auto_ack=False)
        if method_frame is not None:
            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            body_text = body.decode("unicode_escape")
            try:
                request = self._mapper_func(body_text)
                return request
            except TypeError:
                self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

        return None


cancellation_token = object()


def polling_consumer(cancellation_queue: Queue, source_routing_key: str,
                     handler_func: Callable[[Request], Request], mapper_func: Callable[[str], Request],
                     host_name: str= 'localhost') -> None:
    """
    Intended to be called from a thread, we consumer messages in a loop, with a delay between reads from the queue in order
    to allow the CPU to service other requests, including the supervisor which may want to signal that we should quit
    We use a queue to signal cancellation - the cancellation token is put into the queue and a consumer checks for it
    after every loop
    :param handler_func: What should we do with the message we have received
    :param source_routing_key: The routing key that we are the data sink for
    :param cancellation_queue: Used for inter-process communication, push a cancellation token to this to terminate
    :param mapper_func: How do we serialize messages from the wire into a python object
    :param host_name: Where is the RMQ exchange
    :return:
    """
    with Consumer(source_routing_key, mapper_func, host_name) as channel:
        while True:
            message = channel.receive()
            if message is not None:
                handler_func(message)
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


def routing_step(cancellation_queue: Queue, source_routing_key, deserializer_func: Callable[[str], RoutingSlip],
                 operation_func: Callable[[RoutingSlip], RoutingSlip], serializer_func: Callable[[RoutingSlip], str],
                 host_name: str= 'localhost') -> None:
    """
    Intended to be called from a thread, we consumer messages in a loop, with a delay between reads from the queue in order
    to allow the CPU to service other requests, including the supervisor which may want to signal that we should quit
    We use a queue to signal cancellation - the cancellation token is put into the queue and a consumer checks for it
    after every loop
    The routing step differs from a flat polling consumer in that it has both an input queue and an output key. The
    output channel uses the routing key from the message. In this way we differ from pipes and filters, where the
    destination routing key must be decided at design time, via a parameter to the filter() function. In a
    routing slip scenario we decide at runtime instead.
    :param serializer_func: How do we serialize the object as a message back onto the queue
    :param operation_func: The work that this routing step does
    :param cancellation_queue: Used for inter-process communication, push a cancellation token to this to terminate
    :param deserializer_func: How do we serialize messages from the wire into a python object
    :param host_name: Where is the RMQ exchange
    :return:
    """
    with Consumer(source_routing_key, deserializer_func, host_name) as in_channel:
        while True:
            message = in_channel.receive()
            if message:
                nex_step_id = message.current_step + 1
                try:
                    next_step = message.steps[nex_step_id]
                    enriched_message = operation_func(message)
                    enriched_message.current_step = nex_step_id
                    with Producer(next_step.routing_key, serializer_func, host_name) as producer:
                        producer.send(enriched_message)
                        print(f"Sent {enriched_message} to {next_step.routing_key}")
                except IndexError:
                    pass
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