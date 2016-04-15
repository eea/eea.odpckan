""" Proxy - methods and logic to emulate messages
"""

import sys
from random import choice

from config import logger, rabbit_config
from eea.rabbitmq.client import RabbitMQConnector

datasets = {
    'linkages-of-species-and-habitat': 'http://www.eea.europa.eu/data-and-maps/data/linkages-of-species-and-habitat',
    'eea-coastline-for-analysis-1': 'http://www.eea.europa.eu/data-and-maps/data/eea-coastline-for-analysis-1',
    'high-nature-value-farmland': 'http://www.eea.europa.eu/data-and-maps/data/high-nature-value-farmland',
    'predicted-habitat-suitability-for-eunis': 'http://www.eea.europa.eu/data-and-maps/data/predicted-habitat-suitability-for-eunis',
    'national-emissions-reported-to-the-unfccc-and-to-the-eu-greenhouse-gas-monitoring-mechanism-10': 'http://www.eea.europa.eu/data-and-maps/data/national-emissions-reported-to-the-unfccc-and-to-the-eu-greenhouse-gas-monitoring-mechanism-10',
    'article-12-database-birds-directive-2009-147-ec': 'http://www.eea.europa.eu/data-and-maps/data/article-12-database-birds-directive-2009-147-ec',
    'nationally-designated-areas-national-cdda-10': 'http://www.eea.europa.eu/data-and-maps/data/nationally-designated-areas-national-cdda-10',
    'ecosystem-types-of-europe': 'http://www.eea.europa.eu/data-and-maps/data/ecosystem-types-of-europe',
    'biogeographical-regions-europe-3': 'http://www.eea.europa.eu/data-and-maps/data/biogeographical-regions-europe-3',
    'member-states-reporting-art-7-under-the-european-pollutant-release-and-transfer-register-e-prtr-regulation-11': 'http://www.eea.europa.eu/data-and-maps/data/member-states-reporting-art-7-under-the-european-pollutant-release-and-transfer-register-e-prtr-regulation-11',
}

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
        logger.info(
            'START send messages in \'%s\'',
            self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.declare_queue(self.queue_name)
        for idx in range(0, howmany):
            action = choice(actions)
            dataset_identifier, dataset_url = choice(datasets.items())
            body = '%(action)s|%(dataset_url)s|%(dataset_identifier)s' % {
                'action': action,
                'dataset_url': dataset_url,
                'dataset_identifier': dataset_identifier}
            logger.info(
                'SENDING \'%s\' in \'%s\'',
                body,
                self.queue_name)
            self.rabbit.send_message(self.queue_name, body)
        self.rabbit.close_connection()
        logger.info(
            'DONE send messages in \'%s\'',
            self.queue_name)


if __name__ == '__main__':
    #handle parameters
    try:
        howmany = int(sys.argv[1])
    except:
        howmany = 1

    #inject some messages
    cp = ProxyProducer('odp_queue')
    cp.send_messages(howmany)

