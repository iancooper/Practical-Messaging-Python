import pika


class p2p:

    exchange_name = "practical-messaging"

    def __init__(self, queue_name: str, host_name: str='localhost') -> None:
        self._queue_name = queue_name
        self._routing_key = queue_name
        self._connection_parameters = pika.ConnectionParameters(host=host_name)

    def __enter__(self) -> 'p2p':
        self._connection = pika.BlockingConnection(parameters=self._connection_parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=p2p.exchange_name, exchange_type='direct', durable=False, auto_delete=False)
        self._channel.queue_declare(queue=self._queue_name, durable=False, exclusive=False, auto_delete=False)

        return self

    def send(self, message:str) -> None:
        self._channel.basic_publish(exchange=p2p.exchange_name, routing_key=self._routing_key, body=message)

    def receive(self) -> str:
        method_frame, header_frame, body = self._channel.basic_get(queue=self._queue_name, no_ack=True)
        if method_frame is not None:
            return body
        else:
            return None

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._channel.close()
        self._connection.close()


