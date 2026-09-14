# ruff: noqa: BLE001, S110
import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


def _raise(exception: Exception):
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


def _transform(on_message_callback):
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

    return callback


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._channel = None
        self._connection = None

        try:
            self._consuming = False
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host),
            )
            self._channel = self._connection.channel()
            self._queue_name = queue_name
            self._channel.queue_declare(
                queue=self._queue_name,
                durable=False,
                auto_delete=False,
            )
        except Exception as e:
            self._handle_exception(e)

    def _handle_exception(self, exception):
        try:
            self._close()
        except Exception:
            pass
        _raise(exception)

    def _close(self):
        error = None
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
            except Exception as e:
                error = e
            finally:
                self._channel = None
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception as e:
                error = error or e
            finally:
                self._connection = None
        if error:
            raise error

    def _assert_connection(self):
        if not self._connection or not self._connection.is_open:
            raise MessageMiddlewareDisconnectedError()

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        self._assert_connection()
        if self._consuming:
            return

        try:
            self._consuming = True

            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_transform(on_message_callback),
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
    def send(self, message):
        self._assert_connection()
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
            )
        except Exception as e:
            self._handle_exception(e)

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e) from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self._channel = None
        self._connection = None

        try:
            self._consuming = False
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host),
            )
            self._channel = self._connection.channel()
            self._exchange_name = exchange_name

            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="direct",
                durable=False,
            )

            queue = self._channel.queue_declare(
                queue="",
                durable=False,
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
        except Exception as e:
            self._handle_exception(e)

    def _handle_exception(self, exception):
        try:
            self._close()
        except Exception:
            pass
        _raise(exception)

    def _close(self):
        error = None
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
            except Exception as e:
                error = e
            finally:
                self._channel = None
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception as e:
                error = error or e
            finally:
                self._connection = None
        if error:
            raise error

    def _assert_connection(self):
        if not self._connection or not self._connection.is_open:
            raise MessageMiddlewareDisconnectedError()

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control con el cuerpo del mensaje.
    # on_message_callback tiene como parámetros:
    # message - El valor tal y como lo recibe el método send de esta clase.
    # ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
    # nack - Función que al invocarse realiza nack al mensaje que se está consumiendo.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        self._assert_connection()
        if self._consuming:
            return

        try:
            self._consuming = True

            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_transform(on_message_callback),
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
    def send(self, message):
        self._assert_connection()
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except Exception as e:
            self._handle_exception(e)

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e) from e
