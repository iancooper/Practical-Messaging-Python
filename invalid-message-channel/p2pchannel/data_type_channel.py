import pika
from typing import Callable, Type

exchange_name = "practical-messaging-invalid-message"
invalid_message_exchange_name = "practical-messaging-invalid"


class Request:
    pass


class Producer:

    def __init__(self, request_class: Type[Request], mapper_func: Callable[[Request], str], host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        We use the request class to get the name of the routing key. We could use any approach to agree a routing key
        between producer and consumer, but using the name of the type that the queue is of makes a certain sense
        We name the queue after the routing key because we are building a point-to-point channel with a single consudmer
        We split the producer and consumer because one serializes and the other deserializes and we do not wish to make
        a single DataType channel class with both dependencies
        :param request_class: The type on this channel derived from Request
        :param mapper_func: The function that maps
        :param host_name: The name of the host
        """
        self._queue_name = request_class.__name__
        self._routing_key = self._queue_name
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
        self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=False, auto_delete=False)

        invalid_routing_key = 'invalid.' + self._routing_key
        invalid_queue_name = invalid_routing_key

        args = {'x-dead-letter-exchange': invalid_message_exchange_name, 'x-dead-letter-routing-key': invalid_routing_key}

        self._channel.queue_declare(queue=self._queue_name, durable=False, exclusive=False, auto_delete=False, arguments=args)
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
        self._channel.basic_publish(exchange=exchange_name, routing_key=self._routing_key, body=message_body)


class Consumer:

    def __init__(self, request_class: Type[Request], mapper_func: Callable[[str], Request], host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        """
        self._queue_name = request_class.__name__
        self._routing_key = self._queue_name
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
        self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=False, auto_delete=False)

        invalid_routing_key = 'invalid.' + self._routing_key
        invalid_queue_name = invalid_routing_key

        args = {'x-dead-letter-exchange': invalid_message_exchange_name, 'x-dead-letter-routing-key': invalid_routing_key}

        self._channel.queue_declare(queue=self._queue_name, durable=False, exclusive=False, auto_delete=False, arguments=args)
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
        method_frame, header_frame, body = self._channel.basic_get(queue=self._queue_name, no_ack=False)
        if method_frame is not None:
            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            body_text = body.decode("unicode_escape")
            try:
                request = self._mapper_func(body_text)
                return request
            except TypeError:
                self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

        return None



