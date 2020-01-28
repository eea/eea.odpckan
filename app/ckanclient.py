""" ODP CKAN client - middleware between RabbitMQ and ODP
"""

import sys
import argparse

from eea.rabbitmq.client import RabbitMQConnector

from config import logger, dump_rdf, dump_json
from config import rabbit_config, services_config, other_config
from sdsclient import SDSClient
from odpclient import ODPClient

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
        resp = False
        logger.info('START processing message \'%s\' in \'%s\'',
                    body, self.queue_name)
        try:
            action, dataset_url, dataset_identifier = body.split('|')
            # preserver the URL with HTTP
            if dataset_url.startswith('https'):
                dataset_url = dataset_url.replace('https', 'http', 1)
        except Exception, err:
            logger.error('INVALID message format \'%s\' in \'%s\': %s',
                         body, self.queue_name, err)
        else:
            #connect to SDS and read dataset data
            dataset_rdf, dataset_json, msg = self.get_dataset_data(dataset_url, dataset_identifier)
            if dataset_rdf is not None and dataset_json is not None:
                #connect to ODP and handle dataset action
                msg = self.set_dataset_data(action, dataset_identifier, dataset_url, dataset_json)
                if msg:
                    logger.error('ODP ERROR for \'%s\' dataset \'%s\': %s',
                                 action, dataset_url, msg)
                    if msg.lower().endswith('not found.') and body.startswith('update'):
                        logger.info('Retry dataset \'%s\' with CREATE flag', dataset_url)
                        create_body = 'create%s' % body[6:]
                        self.rabbit.send_message(self.queue_name, create_body)
                        ch.basic_ack(delivery_tag = method.delivery_tag)
                        resp = True
                else:
                    #acknowledge that the message was proceesed OK
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    resp = True
                    logger.info('DONE processing message \'%s\' in \'%s\'',
                                body, self.queue_name)
            else:
                logger.error('SDS ERROR for dataset \'%s\': %s',
                             dataset_url, msg)
                logger.info('ERROR processing message')
        return resp

    def get_dataset_data(self, dataset_url, dataset_identifier):
        """ Interrogate SDS and retrieve full data about
            the specified dataset in JSON format. [#68135]
        """
        logger.info('START get dataset data \'%s\' - \'%s\'',
                    dataset_url, dataset_identifier)
        result_rdf, result_json, msg = self.sds.query_dataset(dataset_url,
                                                         dataset_identifier)

        if not msg:
            logger.info('DONE get dataset data \'%s\' - \'%s\'',
                        dataset_url, dataset_identifier)
            return result_rdf, result_json, msg
        else:
            logger.error('FAIL get dataset data \'%s\' - \'%s\': %s',
                         dataset_url, dataset_identifier, msg)
            return None, None, msg

    def set_dataset_data(self, action, dataset_identifier, dataset_url, dataset_json):
        """ Use data from SDS in JSON format and update the ODP [#68136]
        """
        logger.info('START setting \'%s\' dataset data - \'%s\'', action, dataset_url)

        resp, msg = self.odp.call_action(action, dataset_identifier, dataset_json)

        if not msg:
            logger.info('DONE setting \'%s\' dataset data - \'%s\'', action, dataset_url)
            return msg
        else:
            logger.error('FAIL setting \'%s\' dataset data - \'%s\': %s',
                         action, dataset_url, msg)
            return msg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CKANClient')
    parser.add_argument('--debug', '-d', action='store_true', help='create debug file for dataset data from SDS and the builded package for ODP' )
    args = parser.parse_args()

    cc = CKANClient('odp_queue')

    if args.debug:
        urls = [
            'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8',
            'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12',
            'http://www.eea.europa.eu/data-and-maps/data/heat-eutrophication-assessment-tool',
            'http://www.eea.europa.eu/data-and-maps/data/fluorinated-greenhouse-gases-aggregated-data',
            'http://www.eea.europa.eu/data-and-maps/data/marine-litter',
            'http://www.eea.europa.eu/data-and-maps/data/clc-2006-raster-4',
            'http://www.eea.europa.eu/data-and-maps/data/vans-11',
            'http://www.eea.europa.eu/data-and-maps/data/vans-12',
            'http://www.eea.europa.eu/data-and-maps/data/esd-1',
            'http://www.eea.europa.eu/data-and-maps/data/eunis-db',
        ]

        for dataset_url in urls:
            dataset_identifier = dataset_url.split('/')[-1]

            #query dataset data from SDS
            dataset_rdf, dataset_json, msg = cc.get_dataset_data(dataset_url, dataset_identifier)
            assert not msg

            dump_rdf('.debug.1.sds.%s.rdf.xml' % dataset_identifier, dataset_rdf.decode('utf8'))
            dump_json('.debug.2.sds.%s.json.txt' % dataset_identifier, dataset_json)

            ckan_uri = cc.odp.get_ckan_uri(dataset_identifier)
            ckan_rdf = cc.odp.render_ckan_rdf(ckan_uri, dataset_json)
            dump_rdf('.debug.3.odp.%s.rdf.xml' % dataset_identifier, ckan_rdf)

            save_resp = cc.odp.package_save(ckan_uri, ckan_rdf)
            dump_json('.debug.4.odp.%s.save.resp.json.txt' % dataset_identifier, save_resp)

            # TODO delete (currently returns http 500 internal error)
            # delete_resp = cc.odp.package_delete(dataset_identifier)
            # dump_json('.debug.5.odp.%s.delete.resp.json.txt' % dataset_identifier, delete_resp)

    else:
        #read and process all messages from specified queue
        cc.start_consuming_ex()
