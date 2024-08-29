""" module to hold env class and store common instance
"""
import json


class Env:
    """
    """
    def __init__(self):
        with open("queues.env.json", encoding="utf-8") as f:
            self._data = json.load(f)

    def get_env(self):
        """ return run environment
        """
        return self._data["environment"]

    def get_rmq_config(self):
        """ return rmq config
        """
        return self._data[self.get_env() + "_rmq_config"]

    def get_db_config(self):
        """ return db details
        """
        return self._data[self.get_env() + "_db_config"]

    def get_routing_keys(self, instance_name):
        """ return routing key for rmq instance
        """
        return self._data["queues"][instance_name]["routing_keys"]

    def get_queue(self, instance_name):
        """ return queue for rmq instance
        """
        return self._data["queues"][instance_name]["queue"]

env_instance = Env()
