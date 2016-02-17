""" ODP CKAN client - middleware between RabbitMQ and ODP
"""

from config import logger, rabbit_config
from rabbitmq import RabbitMQConnector

class CKANClient:
    """ CKAN Client
    """

    def __init__(self, queue_name):
        """ """
        self.queue_name = queue_name
        self.rabbit = RabbitMQConnector(**rabbit_config)

    def process_messages(self):
        """ Process all the messages from the queue and stop after
        """
        logger.info(
            'START process_messages \'%s\'',
            self.queue_name)
        self.rabbit.open_connection()
        print 'work in progress'
        self.rabbit.close_connection()
        logger.info(
            'DONE process_messages \'%s\'',
            self.queue_name)

    def start_consuming(self):
        """ Start consuming message from the queue.
            It may be interrupted by stopping the script (CTRL+C).
        """
        logger.info(
            'START start_consuming \'%s\'',
            self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.start_consuming(self.queue_name, self.start_consuming_callback)
        self.rabbit.close_connection()
        logger.info(
            'DONE start_consuming \'%s\'',
            self.queue_name)

    def start_consuming_callback(self, ch, method, properties, body):
        print(" [x] WIP Received %r" % body)

if __name__ == '__main__':
    #read messages
    cc = CKANClient('odp_queue')
    cc.start_consuming()
