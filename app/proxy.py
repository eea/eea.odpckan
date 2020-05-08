""" Proxy - methods and logic to emulate messages
"""

import sys
from random import choice

from config import logger, rabbit_config
from eea.rabbitmq.client import RabbitMQConnector

datasets = [
    'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8',
    'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12',
    'http://www.eea.europa.eu/data-and-maps/data/heat-eutrophication-assessment-tool',
    'http://www.eea.europa.eu/data-and-maps/data/fluorinated-greenhouse-gases-aggregated-data-1',
    'http://www.eea.europa.eu/data-and-maps/data/marine-litter',
    'http://www.eea.europa.eu/data-and-maps/data/clc-2006-raster-4',
    'http://www.eea.europa.eu/data-and-maps/data/vans-11',
    'http://www.eea.europa.eu/data-and-maps/data/vans-12',
    'http://www.eea.europa.eu/data-and-maps/data/esd-1',
    'http://www.eea.europa.eu/data-and-maps/data/eunis-db',
]

actions = ['create', 'update', 'delete']

class ProxyProducer:
    """ Proxy Producer: its function is to emulate
        the EEA Portal that sends messages.
    """

    def __init__(self, queue_name):
        """ """
        self.queue_name = queue_name
        self.rabbit = RabbitMQConnector(**rabbit_config)

    def send_messages(self, howmany):
        """ Senf a message to the queue
        """
        logger.info('STARTING to send messages in \'%s\'', self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.declare_queue(self.queue_name)
        for idx in range(0, howmany):
            action = choice(actions)
            dataset_url = choice(datasets)
            body = '%(action)s|%(dataset_url)s|%(dataset_identifier)s' % {
                'action': action,
                'dataset_url': dataset_url,
                'dataset_identifier': '_fake_dataset_identifier_'}
            logger.info('SENDING \'%s\' in \'%s\'', body, self.queue_name)
            self.rabbit.send_message(self.queue_name, body)
        self.rabbit.close_connection()
        logger.info('DONE sending messages in \'%s\'', self.queue_name)


if __name__ == '__main__':
    #handle parameters
    try:
        howmany = int(sys.argv[1])
    except:
        howmany = 1

    #inject some messages
    cp = ProxyProducer('odp_queue')
    cp.send_messages(howmany)

