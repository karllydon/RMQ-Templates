#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=import-error, wrong-import-position
"""Module for publishing"""

import time
from pika import BasicProperties


from publisher import Publisher
from env import env_instance

from credentials import RabbitMQ
from consistent_logging import LoggerClass
from db_connection import DBConnection


class MessageData:
    """This Class provides the basic method framework for presenting data to be published"""

    def __init__(self, data, logger):
        """Set the data source for the Message Data
        :param object data: Instance of a data store that provides message properties
        routing (topic) and payload
        """
        self._data_source = data
        self._logger = logger

    def get_properties(self, routing):
        """Return the message properties for the RabbitMQ message

        :param string exchange: Return messages for this exchange
        :param string routing: This is the default routing. If this
        not to be used then update it
        Returning None indicates that there is nothing to send
        """
        basic_prop = self._data_source.get_message_properties()
        if basic_prop is not None:
            # Update routing from source if required
            source_routing = self._data_source.get_message_routing()
            if source_routing != "":
                routing = source_routing
        self._logger.info(f"Fetching message properties for {routing}")

        return basic_prop, routing

    def get_message(self):
        """Return the message properties for the RabbitMQ message

        Returning None indicates that there is nothing to send
        """

        self._logger.info("Fetching message body")
        return self._data_source.get_message_payload()

    def mark_as_processed(self):
        """Mark the current message as processed

        The calling application assumes this is sucessful
        """

        self._logger.info("Marking current message as processed")
        self._data_source.mark_as_processed()


class MsgData:
    """initialize class with last connection, source data,
    current index and message
    """

    def __init__(self, logger, db):
        """Create the SkuUpdate object"""
        self._lastconnection = 0.0
        self._sourcedata = []
        self._currentindex = -1
        self._current_message = None
        self._logger = logger
        self._db = db

    def fetch_messages(self):
        """Fetch unacked stock status from database"""
        sql_cursor = self._db.get_connection().cursor(buffered=True)
        try:
            sql = "" #string to get unprocessed msgs from db
            sql_cursor.execute(sql)
            # unacked messages need to be sent
            if sql_cursor.rowcount:
                row_headers = [x[0] for x in sql_cursor.description]
                rv = sql_cursor.fetchall()
                for result in rv:
                    self._sourcedata.append(dict(zip(row_headers, result)))
            else:
                self._sourcedata = []

        except Exception as e:
            self._logger.info(f"Error when fetching data: {e}")
            self._sourcedata = []
        finally:
            sql_cursor.reset()
            self._first_message()
            self._lastconnection = time.time()

    def _next_message(self):
        """Iterator over the returned records
        Call to move the current message pointer forwards by one
        Returns True if sucessful and False otherwise
        """

        self._currentindex += 1

        # Check that we aren't at the end of the data
        if self._currentindex >= len(self._sourcedata):
            self._db.close_connection()
            self._currentindex -= 1
            return False

        return True

    def _first_message(self):
        """Reset the iterator"""
        self._currentindex = -1
        self._current_message = None

    def _reset_all_messages(self):
        """Clear messages data and fetch new data on next call"""
        self._sourcedata = []
        self._first_message()

    def get_message_properties(self):
        """Request the next message properties
        :param string exchange: The exchange to which messages are published
        Returns None if no messages is queued, otherwise a BasicProperties object
        """

        # Check if any messages are loaded, if not then load some
        if len(self._sourcedata) == 0:
            self.fetch_messages()

        # Get the first/next message
        if self._next_message():

            # Return the properties
            self._current_message = self._get_properties()

            if self._current_message is not None:
                return self._current_message.properties

        # No messages left so reset and start again
        else:
            self._reset_all_messages()

        return None

    def get_message_routing(self):
        """Request the current message routing"""

        if self._current_message is not None:
            return self._current_message.routing

        return None

    def get_message_payload(self):
        """Request the current message payload"""

        if self._current_message is not None:
            return self._current_message.payload

        return None

    def mark_as_processed(self):
        """Mark the current message as processed (ie published)"""
        try:
            return 0
        except Exception as e:
            # Log some sort of error
            self._logger.info(f"Error when marking as processed: {e}")
            return 1

    def _get_properties(self):
        """Process the source data returning a Basic Properties object
        Returns an object wrapping the properties
        """
        message_id = self._sourcedata[self._currentindex]["message_id"]
        self._logger.info(f"Fetching data for message id {message_id}")

        return MsgStatus(self._sourcedata[self._currentindex], self._logger, self._db)


class MsgStatus:
    """Class to wrap msg details"""

    def __init__(self, data, logger, db):
        """Instance of class is an acknowledgment message
        :param object data: The source data for the acknowledgment
        """
        self._db = db
        self._logger = logger

        # Create the headers, properties and message payload from the source data
        headers = {
            "RequiresAck": False,
            "AckTopic": "",
            "AckQueue": "",
            "AckExchange": "",
        }
        self.properties = BasicProperties(
            app_id="stockbroker",
            content_type="application/json",
            correlation_id=None,
            delivery_mode=2,
            message_id=data["message_id"],
            timestamp=int(time.time()),
            headers=headers,
        )

        sql_cursor = self._db.get_connection().cursor(buffered=True)

        # getting message from database and processing to publish here
        self.payload = ""
        self.routing = ""

        sql_cursor.reset()


def main():
    """initializes the publisher with logging, credentials,
    message data and the necessary message handling utilities"""
    logger = LoggerClass()
    db = DBConnection(logger, env_instance)
    data = MsgData(logger, db)
    rabbit_creds = RabbitMQ("publisher_instance", env_instance)
    published_data = MessageData(data, logger)

    # Connect to RabbitMQ and publish the data received
    pub = Publisher(rabbit_creds, published_data, logger)
    pub.run()


if __name__ == "__main__":
    main()
