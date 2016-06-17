""" ODP CKAN client - middleware between RabbitMQ and ODP
"""

import sys
import argparse
from config import logger, dump_rdf, dump_json
from config import rabbit_config, services_config, other_config
from eea.rabbitmq.client import RabbitMQConnector
from sdsclient import SDSClient
from odpclient import ODPClient

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
        while True:
            method, properties, body = self.rabbit.get_message(self.queue_name)
            if method is None and properties is None and body is None:
                logger.info('Queue is empty \'%s\'.', self.queue_name)
                break
            self.message_callback(self.rabbit.get_channel(), method, properties, body)
        self.rabbit.close_connection()
        logger.info('DONE consuming from \'%s\'', self.queue_name)

    def message_callback(self, ch, method, properties, body):
        """ Callback method for processing a message from the queue.
            If the message is processed ok then acknowledge,
            otherwise don't - the message will be processed again
            at the next run.
        """
        logger.info('START processing message \'%s\' in \'%s\'',
                    body, self.queue_name)
        try:
            action, dataset_url, dataset_identifier = body.split('|')
        except Exception, err:
            logger.error('INVALID message format \'%s\' in \'%s\': %s',
                         body, self.queue_name, err)
        else:
            #connect to SDS and read dataset data
            dataset_rdf, dataset_json, msg = self.get_dataset_data(dataset_url, dataset_identifier)
            if dataset_rdf is not None and dataset_json is not None:
                #connect to ODP and handle dataset action
                msg = self.set_dataset_data(action, dataset_url, dataset_rdf, dataset_json)
                if msg:
                    logger.error('ODP ERROR for \'%s\' dataset \'%s\': %s',
                                 action, dataset_url, msg)
                    if msg.endswith('not found.') and body.startswith('update'):
                        logger.info('Retry dataset \'%s\' with CREATE flag', dataset_url)
                        create_body = 'create%s' % body[6:]
                        self.rabbit.send_message(self.queue_name, create_body)
                        ch.basic_ack(delivery_tag = method.delivery_tag)
                else:
                    #acknowledge that the message was proceesed OK
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                    logger.info('DONE processing message \'%s\' in \'%s\'',
                                body, self.queue_name)
            else:
                logger.error('SDS ERROR for dataset \'%s\': %s',
                             dataset_url, msg)
                logger.info('ERROR processing message')

    def get_dataset_data(self, dataset_url, dataset_identifier):
        """ Interrogate SDS and retrieve full data about
            the specified dataset in JSON format. [#68135]
        """
        logger.info('START get dataset data \'%s\' - \'%s\'',
                    dataset_url, dataset_identifier)
        sds = SDSClient(services_config['sds'], other_config['timeout'], self.queue_name)
        result_rdf, result_json, msg = sds.query_dataset(dataset_url,
                                                         dataset_identifier)

        if not msg:
            logger.info('DONE get dataset data \'%s\' - \'%s\'',
                        dataset_url, dataset_identifier)
            return result_rdf, result_json, msg
        else:
            logger.error('FAIL get dataset data \'%s\' - \'%s\': %s',
                         dataset_url, dataset_identifier, msg)
            return None, None, msg

    def set_dataset_data(self, action, dataset_url, dataset_data_rdf, dataset_json):
        """ Use data from SDS in JSON format and update the ODP. [#68136]
        """
        logger.info('START setting \'%s\' dataset data - \'%s\'', action, dataset_url)

        odp = ODPClient()
        resp, msg = odp.call_action(action, dataset_json, dataset_data_rdf)

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
        dataset_url = 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-eu-ets-data-from-citl-8'
        dataset_identifier = dataset_url.split('/')[-1]

        #query dataset data from SDS
        dataset_rdf, dataset_json, msg = cc.get_dataset_data(dataset_url, dataset_identifier)
        if not msg:
            dump_rdf('.debug.1.sds.%s.rdf.xml' % dataset_identifier, dataset_rdf)
            dump_json('.debug.2.sds.%s.json.txt' % dataset_identifier, dataset_json)

            odp = ODPClient()

            #build the package structure with data from SDS
            package_name, package_data = odp.transformJSON2DataPackage(dataset_json, dataset_rdf)
            dump_json('.debug.3.cc.%s.json.txt' % dataset_identifier, package_data)

            #query and retreive the ODP package data
            package = odp.package_search(prop='identifier', value=dataset_identifier)[0]
            dump_json('.debug.4.odp.package.%s.json.txt' % dataset_identifier, package)

            #merge the ODP package with the package build from SDS data
            package.update(package_data)
            dump_json('.debug.5.cc.package.%s.json.txt' % dataset_identifier, package)

            #update ODP - CAREFULLY WHEN UNCOMMENT THE FOLLOWING LINES- THE ODP DATASET GETS UPDATED!
            #package_response, msg = odp.package_update(package_data)
            #if not msg:
            #    dump_json('.debug.6.odp.package.%s.json.txt' % dataset_identifier, package_response)
    else:
        #read and process all messages from specified queue
        cc.start_consuming_ex()
