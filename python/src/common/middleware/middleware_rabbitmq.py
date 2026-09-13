import pika

from .middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host),
        )
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue=queue_name)
        self._queue_name = queue_name

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        def callback(channel, method, _properties, body):
            tag = method.delivery_tag
            on_message_callback(
                message=body,
                ack=lambda: channel.basic_ack(delivery_tag=tag),
                nack=lambda: channel.basic_nack(delivery_tag=tag),
            )

        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=callback,
        )
        self._channel.start_consuming()

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        self._channel.stop_consuming()

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        self._channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=message,
        )

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self._channel.close()
        self._connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host),
        )
        self._channel = self._connection.channel()

        self._channel.exchange_declare(exchange=exchange_name)
        self._exchange_name = exchange_name

        queue = self._channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
        )
        self._queue_name = queue.method.queue

        self._routing_keys = routing_keys
        for routing_key in self._routing_keys:
            self._channel.queue_bind(
                queue=self._queue_name,
                exchange=self._exchange_name,
                routing_key=routing_key,
            )

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        def callback(channel, method, _properties, body):
            tag = method.delivery_tag
            on_message_callback(
                message=body,
                ack=lambda: channel.basic_ack(delivery_tag=tag),
                nack=lambda: channel.basic_nack(delivery_tag=tag),
            )

        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=callback,
        )
        self._channel.start_consuming()

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        self._channel.stop_consuming()

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        for routing_key in self._routing_keys:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=message,
            )

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self._channel.close()
        self._connection.close()
