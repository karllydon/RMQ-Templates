# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import ssl
import pika


class Publisher(object):
    """This is a publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures. It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.
    """

    EXCHANGE = "message"
    EXCHANGE_TYPE = "topic"
    PUBLISH_INTERVAL = 5
    DURABLE = False
    QUEUE = "text"
    ROUTING_KEY = "example.text"

    SHORT_INTERVAL = 1.0
    LONG_INTERVAL = 30.0

    def __init__(self, creds, publish_data, logger):
        """Setup the publisher object, passing in the RabbitMQ credentials and
        the source of data to be published.
        :param credentials.RabbitMQ creds: The RabbitMQ credentials
        :param object publish_data: The source of data for the messages to be published
        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self.PUBLISH_INTERVAL = self.SHORT_INTERVAL

        self._url = creds.connection_string()

        self._creds = creds

        self.EXCHANGE = creds.get_exchange()
        self.EXCHANGE_TYPE = creds.get_exchange_type()
        self.DURABLE = creds.is_exchange_durable()
        self.QUEUE = creds.get_queue_name()
        self.ROUTING_KEY = creds.get_routing_keys()

        self._data_src = publish_data
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
            connParameters = pika.ConnectionParameters(
                port=self._creds.get_port(),
                host=self._creds.get_host(),
                virtual_host=self._creds.get_virtual_host(),
                credentials=credentials,
                ssl_options=pika.SSLOptions(context),
            )

        else:

            connParameters = pika.URLParameters(self._url)

        return pika.SelectConnection(
            parameters=connParameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

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
        self._logger.error(f"Connection open failed, reopening in 5 seconds: {err}")
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._logger.warning(f"Connection closed, reopening in 5 seconds: {reason}")
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.
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
        self.setup_exchange(self.EXCHANGE)

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
        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        self._logger.warning(f"Channel {channel} was closed: {reason}")
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        self._logger.debug(f"Declaring exchange {exchange_name}")
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            durable=self.DURABLE,
            callback=cb,
        )

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        self._logger.debug(f"Exchange declared: {userdata}")
        self.start_publishing()
        # self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        self._logger.debug(f"Declaring queue {queue_name}")
        self._channel.queue_declare(
            queue=queue_name, durable=self.DURABLE, callback=self.on_queue_declareok
        )

    def on_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        self._logger.debug(
            f"Binding {self.EXCHANGE} to {self.QUEUE} with {self.ROUTING_KEY}"
        )
        self._channel.queue_bind(
            self.QUEUE,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok,
        )

    def on_bindok(self, _unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        self._logger.debug("Queue bound")
        self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ
        """
        self._logger.debug("Issuing consumer related RPC commands")
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.
        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.
        """
        self._logger.debug("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.
        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame
        """
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        self._logger.info(
            f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}",
        )
        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._logger.info(
            f"Published {self._message_number} messages, {len(self._deliveries)} \
                have yet to be confirmed, {self._acked} were acked and {self._nacked} were nacked"
        )

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.
        """
        self._logger.info(
            f"Scheduling next message for {self.PUBLISH_INTERVAL} seconds"
        )
        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL, self.publish_message)

    def publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.
        Once the message has been sent, schedule another message to be sent.
        """
        if self._channel is None or not self._channel.is_open:
            return

        new_routing = self.ROUTING_KEY

        # Fetch properties for messages matching the current exchange. The routing key may change.
        properties, new_routing = self._data_src.get_properties(new_routing)

        if properties is None:

            self.PUBLISH_INTERVAL = self.LONG_INTERVAL

        else:

            message = self._data_src.get_message()

            # foreach allows publishing to multiple routing keys (if is array etc)
            if isinstance(new_routing, list):
                for curr_routing in new_routing:
                    self._channel.basic_publish(
                        self.EXCHANGE, curr_routing, message, properties
                    )
                    self._message_number += 1

            else:
                self._channel.basic_publish(
                    self.EXCHANGE, new_routing, message, properties
                )
                self._message_number += 1

            self._deliveries.append(self._message_number)

            self._data_src.mark_as_processed()

            self._logger.info(f"Published message # {self._message_number}")

            # When a message is published, send the next one as soon as possible
            self.PUBLISH_INTERVAL = self.SHORT_INTERVAL

        self.schedule_next_message()

    def run(self):
        """Run the code by connecting and then starting the IOLoop."""
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if self._connection is not None and not self._connection.is_closed:
                    # Finish closing
                    self._connection.ioloop.start()

        self._logger.info("Stopped")

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.
        """
        self._logger.debug("Stopping")
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.
        """
        if self._channel is not None:
            self._logger.debug("Closing the channel")
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            self._logger.debug("Closing connection")
            self._connection.close()
