""" ODP CKAN client - middleware between RabbitMQ and ODP
"""

import sys
import argparse
import uuid
from pathlib import Path

import jinja2
from eea.rabbitmq.client import RabbitMQConnector

from config import logger, dump_rdf, dump_json
from config import rabbit_config, services_config, other_config
from sdsclient import SDSClient
from odpclient import ODPClient


jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        searchpath=str(Path(__file__).parent / 'templates'),
    ),
    autoescape=jinja2.select_autoescape(['html', 'xml'])
)


class CKANClient:
    """ CKAN Client
    """

    def __init__(self, queue_name):
        """ """
        self.queue_name = queue_name
        self.rabbit = RabbitMQConnector(**rabbit_config)
        self.odp = ODPClient()
        self.sds = SDSClient(services_config['sds'], other_config['timeout'], queue_name, self.odp)

    def process_messages(self):
        """ Process all the messages from the queue and stop after
        """
        logger.info('START to process messages in \'%s\'', self.queue_name)
        self.rabbit.open_connection()
        print 'work in progress'
        self.rabbit.close_connection()
        logger.info('DONE processing messages in \'%s\'', self.queue_name)

    def start_consuming(self):
        """ Start consuming messages from the queue.
            It may be interrupted by stopping the script (CTRL+C).
        """
        logger.info('START consuming from \'%s\'', self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.start_consuming(self.queue_name, self.message_callback)
        self.rabbit.close_connection()
        logger.info('DONE consuming from \'%s\'', self.queue_name)

    def start_consuming_ex(self):
        """ It will consume all the messages from the queue and stops after.
        """
        logger.info('START consuming from \'%s\'', self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.declare_queue(self.queue_name)
        processed_messages = {}
        while True:
            method, properties, body = self.rabbit.get_message(self.queue_name)
            if method is None and properties is None and body is None:
                logger.info('Queue is empty \'%s\'.', self.queue_name)
                break
            if body not in processed_messages:
                flg = self.message_callback(self.rabbit.get_channel(), method, properties, body)
                if flg:
                    processed_messages[body] = 1
            else:
                #duplicate message, acknowledge to skip
                self.rabbit.get_channel().basic_ack(delivery_tag = method.delivery_tag)
                logger.info('DUPLICATE skipping message \'%s\' in \'%s\'',
                            body, self.queue_name)
        self.rabbit.close_connection()
        logger.info('DONE consuming from \'%s\'', self.queue_name)

    def message_callback(self, ch, method, properties, body):
        """ Callback method for processing a message from the queue.
            If the message is processed ok then acknowledge,
            otherwise don't - the message will be processed again
            at the next run.
            Returns True if the messages was processed ok, otherwise False.
        """
        logger.info('START processing message \'%s\' in \'%s\'', body, self.queue_name)
        try:
            action, dataset_url, product_id = body.split('|')
            if action in ['update', 'create']:
                self.publish_dataset(dataset_url, product_id)

            else:
                logger.warning("Unsupported action %r, ignoring", action)

        except Exception:
            logger.exception('ERROR processing message \'%s\' in \'%s\'', body, self.queue_name)
            return False

        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info('DONE processing message \'%s\' in \'%s\'', body, self.queue_name)
            return True

    def get_ckan_uri(self, product_id):
        return u"http://data.europa.eu/88u/dataset/" + product_id

    def render_ckan_rdf(self, data):
        """ Render a RDF/XML that the ODP API will accept
        """
        template = jinja_env.get_template('ckan_package.rdf.xml')
        for resource in data.get('resources', []):
            resource['_uuid'] = str(uuid.uuid4())
        data.update({
            "uuids": {
                "landing_page": str(uuid.uuid4()),
                "contact": str(uuid.uuid4()),
                "contact_homepage": str(uuid.uuid4()),
                "contact_telephone": str(uuid.uuid4()),
                "contact_address": str(uuid.uuid4()),
            }
        })
        return template.render(data)

    def set_dataset_data(self, action, product_id, dataset_url, dataset_rdf):
        """ Use data from SDS in JSON format and update the ODP [#68136]
        """
        logger.info('setting \'%s\' dataset data - \'%s\'', action, dataset_url)
        self.odp.publish_dataset(action, product_id, dataset_rdf, dataset_url)

    def publish_dataset(self, dataset_url, product_id):
        """ Publish dataset to ODP
        """
        logger.info('publish dataset \'%s\'', dataset_url)

        if dataset_url.startswith('https'):
            dataset_url = dataset_url.replace('https', 'http', 1)

        data = self.sds.get_dataset(dataset_url)
        ckan_uri = self.get_ckan_uri(product_id)
        data["uri"] = ckan_uri
        data["product_id"] = product_id
        ckan_rdf = self.render_ckan_rdf(data)
        return self.odp.package_save(ckan_uri, ckan_rdf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CKANClient')
    parser.add_argument('--debug', '-d', action='store_true', help='create debug file for dataset data from SDS and the builded package for ODP' )
    args = parser.parse_args()

    cc = CKANClient('odp_queue')

    if args.debug:
        datasets = [
            # ('DAT-21-en', 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8'),
            ('DAT-21-en', 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12'),
            ('DAT-127-en', 'http://www.eea.europa.eu/data-and-maps/data/fluorinated-greenhouse-gases-aggregated-data-1'),
        ]
        #urls = [
        #    'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8',
        #    'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12',
        #    'http://www.eea.europa.eu/data-and-maps/data/heat-eutrophication-assessment-tool',
        #    'http://www.eea.europa.eu/data-and-maps/data/fluorinated-greenhouse-gases-aggregated-data',
        #    'http://www.eea.europa.eu/data-and-maps/data/marine-litter',
        #    'http://www.eea.europa.eu/data-and-maps/data/clc-2006-raster-4',
        #    'http://www.eea.europa.eu/data-and-maps/data/vans-11',
        #    'http://www.eea.europa.eu/data-and-maps/data/vans-12',
        #    'http://www.eea.europa.eu/data-and-maps/data/esd-1',
        #    'http://www.eea.europa.eu/data-and-maps/data/eunis-db',
        #]

        for product_id, dataset_url in datasets:
            cc.publish_dataset(dataset_url, product_id)

    else:
        #read and process all messages from specified queue
        cc.start_consuming_ex()
