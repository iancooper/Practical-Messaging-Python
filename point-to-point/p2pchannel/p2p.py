import pika


class p2p:

    exchange_name = "practical-messaging-p2p"

    def __init__(self, queue_name: str, host_name: str='localhost') -> None:
        """
        We assume a number of defaults: usr:guest pwd:guest port:5672 vhost: /
        Because we are trying to mirror a point-to-point channel we just make the routing key the queue name
        This means that you always get one queue for messages sent with that routing key, this is as close
        as we get at mirroring true point-to-point when there is a broker in the house
        :param queue_name: The name of the queue we are using to communicate, also used as the routing key
        :param host_name: The name of the host
        """
        self._queue_name = queue_name
        self._routing_key = queue_name
        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'p2p':
        """
        We use a context manager as resources like connections need to be closed
        We return self as the channel is also the send/receive point in this point-to-point scenario
        :return: the point-to-point channel
        """
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=p2p.exchange_name, exchange_type='direct', durable=False, auto_delete=False)
        self._channel.queue_declare(queue=self._queue_name, durable=False, exclusive=False, auto_delete=False)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        We must kill the connection, we chose to kill the channel too
        """
        self._channel.close()
        self._connection.close()

    def send(self, message:str) -> None:
        """
        We use the basic publish approach to sending over the channel. Our routing key is the queue name
        :param message: The message we want to send
        """
        self._channel.basic_publish(exchange=p2p.exchange_name, routing_key=self._routing_key, body=message)

    def receive(self) -> str:
        """
        We just use a basic get on the channel to retrieve the message, and return the body if it
        exists
        :return: The message or None if we could not read from the queue
        """
        method_frame, header_frame, body = self._channel.basic_get(queue=self._queue_name, no_ack=True)
        if method_frame is not None:
            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return body
        else:
            return None



