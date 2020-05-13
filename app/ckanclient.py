""" ODP CKAN client - middleware between RabbitMQ and ODP
"""

import argparse
import uuid
from pathlib import Path

import jinja2
from eea.rabbitmq.client import RabbitMQConnector

from config import logger, rabbit_config, services_config, other_config
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

    def start_consuming_ex(self):
        """ It will consume all the messages from the queue and stops after.
        """
        logger.info('START consuming from \'%s\'', self.queue_name)
        self.rabbit.open_connection()
        self.rabbit.declare_queue(self.queue_name)
        processed_messages = {}
        channel = self.rabbit.get_channel()
        while True:
            method, properties, body = self.rabbit.get_message(self.queue_name)
            if method is None and properties is None and body is None:
                logger.info('Queue is empty \'%s\'.', self.queue_name)
                break
            body_txt = body.decode(properties.content_encoding or 'ascii')
            if body_txt not in processed_messages:
                ok = self.message_callback(body_txt)
                if ok:
                    processed_messages[body_txt] = 1
                    channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # duplicate message, acknowledge to skip
                self.rabbit.get_channel().basic_ack(delivery_tag=method.delivery_tag)
                logger.info('DUPLICATE skipping message \'%s\' in \'%s\'',
                            body_txt, self.queue_name)
        self.rabbit.close_connection()
        logger.info('DONE consuming from \'%s\'', self.queue_name)

    def message_callback(self, body):
        """ Callback method for processing a message from the queue.
            If the message is processed ok then acknowledge,
            otherwise don't - the message will be processed again
            at the next run.
            Returns True if the messages was processed ok, otherwise False.
        """
        logger.info('START processing message \'%s\' in \'%s\'', body, self.queue_name)
        try:
            action, dataset_url, _dataset_identifier = body.split('|')
            if action in ['update', 'create']:
                self.publish_dataset(dataset_url)

            else:
                logger.warning("Unsupported action %r, ignoring", action)

        except Exception:
            logger.exception('ERROR processing message \'%s\' in \'%s\'', body, self.queue_name)
            return False

        logger.info('DONE processing message \'%s\' in \'%s\'', body, self.queue_name)
        return True

    def get_ckan_uri(self, product_id):
        return "http://data.europa.eu/88u/dataset/" + product_id

    def get_odp_eurovoc_concepts(self, product_id):
        package = self.odp.package_show("sfdsafafsfasfsa")
        if package is None:
            return []
        return [i["uri"] for i in package["dataset"]["subject_dcterms"]]

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

    def publish_dataset(self, dataset_url):
        """ Publish dataset to ODP
        """
        logger.info('publish dataset \'%s\'', dataset_url)

        if dataset_url.startswith('https'):
            dataset_url = dataset_url.replace('https', 'http', 1)

        latest_dataset_url = self.sds.get_latest_version(dataset_url)
        data = self.sds.get_dataset(latest_dataset_url)
        product_id = data["product_id"]
        ckan_uri = self.get_ckan_uri(product_id)
        data["uri"] = ckan_uri

        concepts = set(data["concepts_eurovoc"])
        concepts.update(set(self.get_odp_eurovoc_concepts(product_id)))
        data["concepts_eurovoc"] = sorted(concepts)

        ckan_rdf = self.render_ckan_rdf(data)
        self.odp.package_save(ckan_uri, ckan_rdf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CKANClient')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='create debug file for dataset data from SDS and the builded package for ODP')
    args = parser.parse_args()

    cc = CKANClient('odp_queue')

    if args.debug:
        urls = [
            # 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8',
            'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12',
            # 'http://www.eea.europa.eu/data-and-maps/data/heat-eutrophication-assessment-tool',
            'http://www.eea.europa.eu/data-and-maps/data/fluorinated-greenhouse-gases-aggregated-data-1',
            # 'http://www.eea.europa.eu/data-and-maps/data/marine-litter',
            # 'http://www.eea.europa.eu/data-and-maps/data/clc-2006-raster-4',
            # 'http://www.eea.europa.eu/data-and-maps/data/vans-11',
            # 'http://www.eea.europa.eu/data-and-maps/data/vans-12',
            # 'http://www.eea.europa.eu/data-and-maps/data/esd-1',
            # 'http://www.eea.europa.eu/data-and-maps/data/eunis-db',
        ]

        for dataset_url in urls:
            cc.publish_dataset(dataset_url)

    else:
        # read and process all messages from specified queue
        cc.start_consuming_ex()
