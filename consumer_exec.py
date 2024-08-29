#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=import-error, wrong-import-position
""" module for consuming of messages, with auditing and post consume processing functionality
"""

from env import env_instance
from credentials import RabbitMQ
from consumer import ReconnectingConsumer
from consistent_logging import LoggerClass
from db_connection import DBConnection


class Auditor:
    """class for auditing messages, methods for logging checking checking validity etc go here"""

    def __init__(self, logger, db):
        """Setup the auditor

        :param object config: Provide configuration data
        """
        self._logger = logger
        self._db = db

    def log_message_id(self, message_id):
        """Record a message id

        Return 0 on success or > 0 on fail
        """
        self._logger.info(f"Message consumed with ID: {str(message_id)}")
        return 0

    def check_message_id(self, message_id):
        """check if message is already consumed"""
        try:
            return 0
        except Exception as err:
            self._logger.error(f"Error auditing message: {err}")
            return 2


class Helper:
    """class for processing of messages, any additional functions
    for processing once message consumed can be added here"""

    def __init__(self, logger, db):
        self._logger = logger
        self._db = db

    def process_msg(self, properties, message):
        """for processing of messages"""
        try:
            # actual message processing implemenation should be placed here
            return 0
        except Exception as err:
            self._logger.error(f"Error when processinng message: {err}")
            return 1


class MsgHandler:
    """the message handler class is instantiated
    with an auditor and helper for processing of messages once consumed
    """

    def __init__(self, auditor, logger, db):
        """set up handler for connection to target system for processing message
        :param object auditor: Auditor will validate whether the message needs processing
        :param Acknowledger.MsgAcknowledger acknowledger: queue acknowledgment to application
        """
        self._auditor = auditor
        self._logger = logger
        self._helper = Helper(logger, db)

    def process_message(self, properties, message):
        """perform actions are on new message.
        return 0 when sucessfully processed to indicate
        to the caller that the message can be acknowledged to RabbitMQ
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        self._logger.info(f"Auditing message with: {message}")
        if self._auditor.check_message_id(properties.message_id) == 0:
            self._logger.info("Message audited successfully")
            self._auditor.log_message_id(properties.message_id)
            self._logger.info(f"Processing message: {str(properties.message_id)}")
            self._logger.info(
                f"Sent request to propagate message: {str(properties.message_id)}"
            )
            self._helper.process_msg(message, properties.message_id)
        else:
            self._logger.info("Message already consumed")
        return 0


def main():
    """initialize the consumer with db access, logging and message handling"""
    logger = LoggerClass()
    db = DBConnection(logger, env_instance)
    rabbit_creds = RabbitMQ("consumer_instance", env_instance)
    auditor = Auditor(logger, db)
    handler = MsgHandler(auditor, logger, db)
    consumer = ReconnectingConsumer(rabbit_creds, handler, logger)
    consumer.run()


if __name__ == "__main__":
    main()
