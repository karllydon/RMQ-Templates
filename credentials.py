""" credentials module to interface connection with RMQ
"""

class RabbitMQ:
    """This Class holds the RabbitMQ connection credentials along with the
    default exchange, exchange type, routing key and queue name for the consumer
    """

    def __init__(self, instance_name, env_instance):

        self._config = env_instance.get_rmq_config()
        self._routing_keys = env_instance.get_routing_keys(instance_name)
        self._queue = env_instance.get_queue(instance_name)

    def connection_string(self):
        """Create an amqp connection string from the RabbitMQ credentials"""
        return (
            "amqp://"
            + self._config["username"]
            + ":"
            + self._config["password"]
            + "@"
            + self._config["host"]
            + ":"
            + self._config["port"]
            + "/%2F"
        )

    def get_host(self):
        """Return the default host name"""
        return self._config["host"]

    def get_port(self):
        """Return the default port number"""
        return self._config["port"]

    def get_virtual_host(self):
        """Return the virtual host"""
        return self._config["virtual_host"]

    def get_username(self):
        """Return the default username"""
        return self._config["username"]

    def get_password(self):
        """Return the default password"""
        return self._config["password"]

    def uses_ssl(self):
        """Return true if ssl is required, otherwise false"""
        return self._config["uses_ssl"]

    def get_exchange(self):
        """Return the default exchange name"""
        return self._config["exchange"]

    def get_exchange_type(self):
        """Return the default exchange type"""
        return self._config["exchange_type"]

    def is_exchange_durable(self):
        """Return the exchange durability"""
        return self._config["durable"]

    def get_routing_keys(self):
        """Return the default routing keys"""
        return self._routing_keys

    def get_queue_name(self):
        """Return the default queue name"""
        return self._queue
