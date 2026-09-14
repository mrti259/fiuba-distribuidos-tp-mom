# ruff: noqa: BLE001
import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class _RabbitMQBase:
    def __init__(self, host):
        self._consuming = False
        self._channel = None
        self._connection = None
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host),
            )
            self._channel = self._connection.channel()
        except Exception as e:
            self._handle_exception(e)

    def _assert_connection(self):
        try:
            if self._connection and self._connection.is_open:
                return
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(e) from e
        raise MessageMiddlewareDisconnectedError()

    def _close(self, raise_error):
        error = None
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
        except Exception as e:
            error = error or e

        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            error = error or e

        self._channel = None
        self._connection = None
        if error and raise_error:
            raise MessageMiddlewareCloseError(error) from error

    def _handle_exception(self, exception):
        self._close(raise_error=False)
        self._raise(exception)

    def _raise(self, exception: Exception):
        if isinstance(
            exception,
            (
                MessageMiddlewareCloseError,
                MessageMiddlewareDisconnectedError,
                MessageMiddlewareMessageError,
            ),
        ):
            raise exception

        if isinstance(exception, pika.exceptions.AMQPConnectionError):
            raise MessageMiddlewareDisconnectedError(exception) from exception

        if isinstance(exception, pika.exceptions.AMQPChannelError):
            raise MessageMiddlewareMessageError(exception) from exception

        raise MessageMiddlewareMessageError(exception) from exception

    def setup_queue(self, queue_name):
        try:
            self._assert_connection()
            self._channel.queue_declare(
                queue=queue_name,
                durable=False,
                auto_delete=False,
            )
        except Exception as e:
            self._handle_exception(e)

    def setup_exchange(self, exchange_name, routing_keys):
        try:
            self._assert_connection()
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=False,
            )

            queue = self._channel.queue_declare(
                queue="",
                durable=False,
                exclusive=True,
                auto_delete=True,
            )
            queue_name = queue.method.queue

            for routing_key in routing_keys:
                self._channel.queue_bind(
                    queue=queue_name,
                    exchange=exchange_name,
                    routing_key=routing_key,
                )
            return queue_name
        except Exception as e:
            self._handle_exception(e)

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback, queue_name):
        self._assert_connection()
        if self._consuming:
            return

        self._consuming = True
        try:

            def callback(channel, method, _properties, body):
                tag = method.delivery_tag
                on_message_callback(
                    message=body,
                    ack=lambda: channel.basic_ack(delivery_tag=tag),
                    nack=lambda: channel.basic_nack(
                        delivery_tag=tag,
                        requeue=False,
                    ),
                )

            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False,
            )
            self._channel.start_consuming()
        except Exception as e:
            self._handle_exception(e)
        finally:
            self._consuming = False

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        self._assert_connection()
        try:
            if self._consuming:
                self._channel.stop_consuming()
                self._consuming = False
        except Exception as e:
            self._handle_exception(e)

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message, exchange_name, routing_keys):
        self._assert_connection()
        try:
            for routing_key in routing_keys:
                self._channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except Exception as e:
            self._handle_exception(e)

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        self._close(raise_error=True)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._base = _RabbitMQBase(host=host)
        self._queue_name = queue_name
        self._base.setup_queue(queue_name)

    def start_consuming(self, on_message_callback):
        self._base.start_consuming(
            on_message_callback=on_message_callback,
            queue_name=self._queue_name,
        )

    def stop_consuming(self):
        self._base.stop_consuming()

    def send(self, message):
        self._base.send(
            message=message,
            exchange_name="",
            routing_keys=[self._queue_name],
        )

    def close(self):
        self._base.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self._base = _RabbitMQBase(host=host)
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._queue_name = self._base.setup_exchange(
            exchange_name,
            routing_keys,
        )

    def start_consuming(self, on_message_callback):
        self._base.start_consuming(
            on_message_callback=on_message_callback,
            queue_name=self._queue_name,
        )

    def stop_consuming(self):
        self._base.stop_consuming()

    def send(self, message):
        self._base.send(
            message=message,
            exchange_name=self._exchange_name,
            routing_keys=self._routing_keys,
        )

    def close(self):
        self._base.close()
