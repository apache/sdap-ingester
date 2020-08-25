from aio_pika import Message, DeliveryMode, Connection, Channel, connect_robust
from tenacity import retry, stop_after_attempt, wait_fixed


class MessagePublisher:

    def __init__(self, host: str, username: str, password: str, queue: str):
        self._connection_string = f"amqp://{username}:{password}@{host}/"
        self._queue = queue
        self._channel: Channel = None
        self._connection: Connection = None

    async def __aenter__(self):
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close()

    async def _connect(self):
        """
        Establish a connection to RabbitMQ.
        :return: None
        """
        self._connection = await connect_robust(self._connection_string)
        self._channel = await self._connection.channel()
        await self._channel.declare_queue(self._queue, durable=True, arguments={'x-max-priority': 10})

    @retry(wait=wait_fixed(5), reraise=True, stop=stop_after_attempt(4))
    async def publish_message(self, body: str, priority: int = None):
        """
        Publish a message to RabbitMQ using the optional message priority.
        :param body: A string to publish to RabbitMQ
        :param priority: An optional integer priority to use for the message
        :return: None
        """
        message = Message(body=body.encode('utf-8'), priority=priority, delivery_mode=DeliveryMode.PERSISTENT)
        await self._channel.default_exchange.publish(message, routing_key=self._queue)

