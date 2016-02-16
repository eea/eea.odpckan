""" RabbitMQ connector - methods to work with the RabbitMQ service
"""

import pika

from config import logger

class RabbitMQConnector:
    """ RabbitMQ connector
    """

    def __init__(self, rabbit_host, rabbit_port, rabbit_username,
        rabbit_password):
        """ """
        self.__rabbit_connection = None
        self.__rabbit_channel = None
        self.__rabbit_host = rabbit_host
        self.__rabbit_port = rabbit_port
        self.__rabbit_credentials = pika.PlainCredentials(rabbit_username,
            rabbit_password)

    def open_connection(self):
        """ Connect to service and open a channel
        """
        try:
            self.__rabbit_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.__rabbit_host,
                    port=self.__rabbit_port,
                    credentials=self.__rabbit_credentials))
            self.__rabbit_channel = self.__rabbit_connection.channel()
        except Exception, err:
            logger.error(
                'Connecting to RabbitMQ at %s:%s faild with error: %s',
                self.__rabbit_host,
                self.__rabbit_port,
                err)
        else:
            logger.info(
                'Connecting to RabbitMQ at %s:%s OK',
                self.__rabbit_host,
                self.__rabbit_port)

    def close_connection(self):
        """ Disconnect from service and kill the channel
        """
        try:
            self.__rabbit_connection.close()
            self.__rabbit_connection = None
            self.__rabbit_channel = None
        except Exception, err:
            logger.error(
                'Disconnecting from RabbitMQ at %s:%s faild with error: %s',
                self.__rabbit_host,
                self.__rabbit_port,
                err)
        else:
            logger.info(
                'Disconnecting from RabbitMQ at %s:%s OK',
                self.__rabbit_host,
                self.__rabbit_port)

    def get_queue_status(self, queue_name):
        """ Get the specified queue status
        """
        return self.__rabbit_channel.queue_declare(queue=queue_name,
            passive=True)

    def is_queue_empty(self, queue_name):
        """ Check is the specified queue is empty
        """
        status = self.get_queue_status(queue_name)
        is_empty = status.method.message_count == 0
        logger.info(
            'Queue %s is empty = %s',
            queue_name,
            is_empty)
        return is_empty

    def start_consuming(self, queue_name, callback):
        """ Start consuming message from the queue.
            It may be interrupted by stopping the script (CTRL+C).
        """
        self.__rabbit_channel.queue_declare(queue=queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False)
        self.__rabbit_channel.basic_consume(callback,
            queue=queue_name)
        logger.info(
            'Waiting for messages \'%s\'. To exit press CTRL+C',
            queue_name)
        self.__rabbit_channel.start_consuming()

    def send_message(self, queue_name, body):
        """ Send a message to the queue.
            We use the default exchange and route through
            the queue name.
        """
        self.__rabbit_channel.queue_declare(queue=queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False)
        self.__rabbit_channel.basic_publish(exchange='',
            routing_key=queue_name,
            body=body)
        logger.info(
            'Message sent \'%s\' in \'%s\'',
            body,
            queue_name)
