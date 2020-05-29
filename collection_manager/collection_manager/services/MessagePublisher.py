import pika


class MessagePublisher:

    def __init__(self, host: str, username: str, password: str, queue: str):
        self._connection_string = f"amqp://{username}:{password}@{host}/"
        self._queue = queue
        self._channel = None
        self._connection = None

    def connect(self):
        """
        Establish a connection to RabbitMQ.
        :return: None
        """
        parameters = pika.URLParameters(self._connection_string)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(self._queue, durable=True)

    def publish_message(self, body: str, priority: int = None):
        """
        Publish a message to RabbitMQ using the optional message priority.
        :param body: A string to publish to RabbitMQ
        :param priority: An optional integer priority to use for the message
        :return: None
        """
        properties = pika.BasicProperties(content_type='text/plain',
                                          delivery_mode=1,
                                          priority=priority)
        self._channel.basic_publish(exchange='',
                                    routing_key=self._queue,
                                    body=body,
                                    properties=properties)

    def __del__(self):
        if self._connection:
            self._connection.close()
