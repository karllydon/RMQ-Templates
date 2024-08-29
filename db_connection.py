""" Module for common db interface for consume/get messages to publish
"""

import mysql
import mysql.connector
from mysql.connector.constants import ClientFlag


class DBConnection:
    """DB connection class"""

    def __init__(self, logger, env_instance):
        """Create DBConnection object"""
        self._logger = logger
        env = env_instance.get_db_config()
        self._config = {
            "host": env["host"],
            "user": env["user"],
            "password": env["password"],
            "database": env["name"],
            "client_flags": [ClientFlag.SSL],
            "ssl_ca": env["ssl_ca"],
            "ssl_cert": env["ssl_cert"],
            "ssl_key": env["ssl_key"],
        }
        self._logger.debug(f"db config {self._config}")
        self._connection = mysql.connector.connect(**self._config)
        self._logger.debug("connected")

    def get_connection(self):
        """retrieve the connection, create new if does not exist"""
        if not self._connection.is_connected():
            self._connection = self.new_connection()
        return self._connection

    def new_connection(self):
        """create new connection"""
        self._logger.debug("new connection")
        return mysql.connector.connect(**self._config)

    def close_connection(self):
        """close when done"""
        self._logger.debug("close connection")
        self._connection.close()

    def log_action(self, app, action):
        """log consume/publish actions to db"""
        conn = self.get_connection()
        cursor = conn.cursor()
        sql = "INSERT INTO action_log (app,action) VALUES (%s,%s);"
        val = (app, action)
        cursor.execute(sql, val)
        conn.commit()
        cursor.close()

    def log_failed_msg(self, message_id, properties, body, reason):
        """log failed message consumes to db"""
        conn = self.get_connection()
        cursor = conn.cursor()
        sql = "INSERT INTO msg_failed_dump (message_id, properties, body, reason) \
            VALUES (%s,%s, %s, %s);"
        val = (message_id, properties, body, reason)
        cursor.execute(sql, val)
        conn.commit()
        cursor.close()
