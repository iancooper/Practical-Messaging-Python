from datetime import datetime, timedelta
import json
import pika
from queue import Empty, Queue
import time
from typing import Callable, Type
from uuid import UUID, uuid4

exchange_name = "practical-messaging-request-reply"
invalid_message_exchange_name = "practical-messaging-invalid"


# To help with JSON serialization, don't use properties in these types

class Request:
    def __init__(self):
        self.reply_to = None
        self.correlation_id = None


class Response:
    def __init__(self, correlation_id: UUID=None) -> None:
        self.correlation_id = correlation_id


class Producer:

    def __init__(self,
                 request_class: Type[Request],
                 serializer_func: Callable[[Request], str],
                 deserializer_func: Callable[[str], Response],
                 host_name: str= 'localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        We use the request class to get the name of the routing key. We could use any approach to agree a routing key
        between producer and consumer, but using the name of the type that the queue is of makes a certain sense
        We name the queue after the routing key because we are building a point-to-point channel with a single consudmer
        We split the producer and consumer because one serializes and the other deserializes and we do not wish to make
        a single DataType channel class with both dependencies
        :param request_class: The type on this channel derived from Request
        :param serializer_func: The function that maps
        :param host_name: The name of the host
        """
        self._queue_name = request_class.__name__
        self._routing_key = self._queue_name
        self._serializer_func = serializer_func
        self._deserializer_func = deserializer_func
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

    def call(self, message: Request, timeout_milliseconds: int) -> Response:
        """
        Call another process and wait for the response. This blocks, as it has function call semantics
        We have two choices: (a) a queue per call. This has overhead but makes correlation of message
        between call and response trivial; (b) a queue per client, we would need to correlate responses to
        ensure we handled out-of-order messages (might be enough to drop ones we don't recognize). We block
        awaiting the response as that is an RPC semantic, over allowing a seperate consumer to receive responses
        and handle them via a handler. That alternative uses routing keys over queues to work and is less true RPC
        than request-reply

       :param message: The message we want to send
        """


        # TODO declare a queue for replies, we can auto-delete this as it should die with us
        result = self._channel.queue_declare(durable=False, exclusive=True, auto_delete=True)
        # TODO auto-generate a queue name; we don't need a routing key as we just send/receive from this queue
        callback_queue_name = result.method.queue

        # Note that we do not need bind to the default exchange; any queue declared on the default exchange
        # automatically has a routing key that is the queue name. Because we choose a random
        # queue name this means we avoid any collisions

        # TODO: serialize the message body
        message_body = self._serializer_func(message)

        # TODO: publish. on On
        # TODO: the basic properties for the message, set the callback queue,and persistence, and a correlation id
        self._channel.basic_publish(exchange=exchange_name, routing_key=self._routing_key, body=message_body,
                                    properties=pika.BasicProperties(delivery_mode=2, reply_to=callback_queue_name, correlation_id=str(uuid4()))
                                    )

        """
        while we are not past the timeout
            try to read from the queue
            if we have a message then
                ack the message 
                decode the message hint: unicode escape
                deserialize into a response type
            else
                yield for a short interval but not too long, remember the timeout
                
        delete the queue when done
        return the response
                
            
        """



class RequestReplyConsumer:

    exchange_name = "practical-messaging-fanout"

    def __init__(self, request_class: Type[Request], mapper_func: Callable[[str], Request], host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        """
        self._queue_name = request_class.__name__
        self._routing_key = self._queue_name
        self._mapper_func = mapper_func

        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'RequestReplyConsumer':
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

        """
            TODO:
            Deserialize the message
            If you have a message then
                Ack the message
                Decode the message body hint: unicode_escape
                Deserialize the message into a request type 
                Set the reply to header on the response properties to the request
                Set the correlation id on the request
        """

class RequestReplyResponder:
    def __init__(self, serializer_func: Callable[[Response], str], host_name: str= 'localhost'):
        self._serializer_func= serializer_func
        self._host_name = host_name

        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'RequestReplyResponder':
        """
        We use a context manager as resources like connections need to be closed
        :return: the responder
        """
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        We must kill the connection, we chose to kill the channel too
        """
        self._channel.close()
        self._connection.close()

    def respond(self, reply_to: str, response: Response) -> None:
        """
        On the default exchange the queue name is the routing key, so we pass the reply to queue name as the routing key
        when we publish
        :param reply_to: The name of the queue we are replying on
        :param response: The response
        :return: None
        """

        # Serialize the message body from the response
        # publish the message


cancellation_token = object()


def polling_consumer(cancellation_queue: Queue, request_class: Type[Request], serializer_func: Callable[[str], Request],
                     handler_func: Callable[[Request], Response], deserializer_func: Callable[[Response], str],
                     host_name: str= 'localhost') -> None:
    """
    Intended to be called from a thread, we consumer messages in a loop, with a delay between reads from the queue in order
    to allow the CPU to service other requests, including the supervisor which may want to signal that we should quit
    We use a queue to signal cancellation - the cancellation token is put into the queue and a consumer checks for it
    after every loop
    :param deserializer_func: How do we serialize messages from Python to the wire
    :param handler_func: The callback that handles the message received from the producer and creates a response
    :param cancellation_queue: Used for inter-process communication, push a cancellation token to this to terminate
    :param request_class: What is the type of message we expect to receive on this channel
    :param serializer_func: How do we de-serialize messages from the wire into a python object
    :param host_name: Where is the RMQ exchange
    :return:
    """
    with RequestReplyConsumer(request_class, serializer_func, host_name) as channel:
        while True:

            """
                Try to receive a message on the channel
                If we get a message then:
                    handle the message 
                    create a request responder hint: with
                        respond with the response from the handler, to the reply_to address
            """

            # This will block whilst it waits for a cancellation token; we don't want to wait long
            try:
                token = cancellation_queue.get(block=True, timeout=0.1)
                if token is cancellation_token:
                    print("Stop instruction received")
                    break
            except Empty:
                time.sleep(0.5)  # yield between messages
                continue




