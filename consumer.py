""" Base Consumer module
"""

import functools
import time
import ssl
import pika


class ReconnectingConsumer:
    """This consumer will reconnect if the nested
    Consumer indicates that a reconnect is necessary.
    """

    def __init__(self, creds, msg_handler, logger):
        self._reconnect_delay = 0
        self._credentials = creds
        self._handler = msg_handler
        self._logger = logger
        self._consumer = Consumer(creds, msg_handler, logger)

    def run(self):
        """initializes the consumer event loop"""
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            self._logger.info(f"Reconnecting after {reconnect_delay} seconds")
            time.sleep(reconnect_delay)
            self._consumer = Consumer(self._credentials, self._handler, self._logger)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        self._reconnect_delay = min(self._reconnect_delay, 30)
        return self._reconnect_delay


class Consumer:
    """consumer that will handle unexpected interactions
    with rmq such as channel and connection closures.
    If rmq closes the connection, this class will stop and indicate
    that reconnection is necessary."""

    exchange = "message"
    exchange_type = ""
    queue = "text"
    routing_keys = ["example.text"]
    durable = False

    def __init__(self, creds, msg_handler, logger):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param object creds: Connection object containing
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = creds.connection_string()

        self._creds = creds

        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self.exchange = creds.get_exchange()
        self.exchange_type = creds.get_exchange_type()
        self.durable = creds.is_exchange_durable()
        self.queue = creds.get_queue_name()
        self.routing_keys = creds.get_routing_keys()

        self._handler = msg_handler
        self._logger = logger

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        self._logger.debug(f"Connecting to {self._url}")

        if self._creds.uses_ssl():

            credentials = pika.credentials.PlainCredentials(
                self._creds.get_username(), self._creds.get_password()
            )
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            conn_parameters = pika.ConnectionParameters(
                port=self._creds.get_port(),
                host=self._creds.get_host(),
                virtual_host=self._creds.get_virtual_host(),
                credentials=credentials,
                ssl_options=pika.SSLOptions(context),
            )

        else:

            conn_parameters = pika.URLParameters(self._url)

        return pika.SelectConnection(
            parameters=conn_parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self):
        """close the connection to RMQ (or report if unexpectandly closed)"""
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._logger.debug("Connection is closing or already closed")
        else:
            self._connection.close()
            self._logger.debug("Closing connection")

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        self._logger.debug("Connection opened")
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        self._logger.error(f"Connection open failed: {err}")
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning(f"Connection closed, reconnect necessary: {reason}")
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        self._logger.debug("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        self._logger.debug("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        self._logger.debug("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        self._logger.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        self._logger.debug(f"Declaring exchange: {exchange_name}")
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
            callback=cb,
        )

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        self._logger.debug(f"Exchange declared: {userdata}")
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        self._logger.debug(f"Declaring queue {queue_name}")
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, durable=self.durable, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        queue_name = userdata
        self._logger.debug(
            f"Binding {self.exchange} to {queue_name} with {self.routing_keys}"
        )
        cb = functools.partial(self.on_bindok, userdata=queue_name)

        for routing_key in self.routing_keys:
            self._channel.queue_bind(
                queue_name, self.exchange, routing_key=routing_key, callback=cb
            )
            cb = None

    def on_bindok(self, _unused_frame, userdata):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.
        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        self._logger.debug(f"Queue bound: {userdata}")
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok
        )

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        self._logger.debug(f"QOS set to: {self._prefetch_count}")
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        self._logger.debug("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.queue, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        self._logger.debug("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        self._logger.debug(
            f"Consumer was cancelled remotely, shutting down: {method_frame}"
        )
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        self._logger.info(
            f"Received message # {basic_deliver.delivery_tag} from {properties.app_id}: {body}",
        )

        # Process the message with the message handler
        result = self._handler.process_message(properties, body)
        # Check for positive result before acknowledging the message
        if result == 0:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        self._logger.info(f"Acknowledging message {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            self._logger.debug("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        self._logger.debug(
            f"RabbitMQ acknowledged the cancellation of the consumer: {userdata}"
        )
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        self._logger.debug("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            self._logger.debug("Stopping")
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._logger.info("Stopped")
