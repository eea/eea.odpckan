""" SDS client - methods to query/get a dataset data from
    SDS (http://semantic.eea.europa.eu/) 
"""

import sys
import argparse
import urllib, urllib2
import rdflib
import json

from eea.rabbitmq.client import RabbitMQConnector

from config import logger, dump_rdf, dump_json
from config import services_config, rabbit_config, other_config
from odpclient import ODPClient

class SDSClient:
    """ SDS client
    """

    def __init__(self, endpoint, timeout, queue_name, odp):
        """ """
        self.endpoint = endpoint
        self.timeout = timeout
        self.queue_name = queue_name
        self.odp = odp

    def is_int(self, text):
        """ Check if text is a number
        """
        ret_val = False
        try:
            val = int(text)
            ret_val = True
        except:
            pass
        return ret_val

    def parse_datasets_json(self, datasets_json):
        """ Parses a response with datasets from SDS in JSON format.
        """
        r = []
        for item_json in datasets_json['results']['bindings']:
            product_id = item_json['product_id']['value']
            dataset_url = item_json['dataset']['value']
            r.append((dataset_url, product_id))
        return r

    def validate_result(self, dataset_rdf, dataset_url):
        """ Validates that the given dataset result is complete.
            We are gone make we of the ODPClient's method
        """
        self.odp.process_sds_result(dataset_rdf, dataset_url)

    def query_sds(self, query, format):
        """ Generic method to query SDS to be used all around.
        """
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        urllib2.install_opener(opener)
        data = urllib.urlencode({"query": query, "format": format})
        req = urllib2.Request(self.endpoint, data=data)
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        req.add_header('Accept', format)
        conn = urllib2.urlopen(req, timeout=self.timeout)
        resp = conn.read()
        conn.close()
        return resp

    def query_dataset(self, dataset_url, product_id):
        """ Given a dataset URL interogates the SDS service
            about it and returns the result which is RDF.
            The RDF result will be converted also to JSON.
        """
        logger.info('query dataset \'%s\' - \'%s\'', dataset_url, product_id)
        query = other_config['query_dataset'] % {"dataset": dataset_url}
        result_rdf = self.query_sds(query, 'application/xml')
        self.validate_result(result_rdf, dataset_url)
        return result_rdf

    def query_all_datasets(self):
        """ Find all datasets (to pe updated in ODP) in the repository.
        """
        logger.info('query all datasets')
        query = other_config['query_all_datasets']
        result = self.query_sds(query, 'application/json')
        return json.loads(result)

    def get_rabbit(self):
        rabbit = RabbitMQConnector(**rabbit_config)
        rabbit.open_connection()
        rabbit.declare_queue(self.queue_name)
        return rabbit

    def add_to_queue(self, rabbit, action, dataset_url, product_id, counter=1):
        body = '%(action)s|%(dataset_url)s|%(product_id)s' % {
            'action': action,
            'dataset_url': dataset_url,
            'product_id': product_id}
        logger.info('BULK update %s: sending \'%s\' in \'%s\'', counter, body, self.queue_name)
        rabbit.send_message(self.queue_name, body)

    def bulk_update(self):
        """ Queries SDS for all datasets and injects messages in rabbitmq.
        """
        logger.info('START bulk update')
        result_json = self.query_all_datasets()
        datasets_json = result_json['results']['bindings']
        logger.info('BULK update: %s datasets found', len(datasets_json))
        rabbit = self.get_rabbit()
        counter = 1
        for item_json in datasets_json:
            product_id = item_json['product_id']['value']
            dataset_url = item_json['dataset']['value']
            action = 'update'
            self.add_to_queue(rabbit, action, dataset_url, product_id, counter)
            counter += 1
        rabbit.close_connection()
        logger.info('DONE bulk update')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SDSClient')
    parser.add_argument('--debug', '-d', action='store_true', help='creates debug files for datasets queries' )
    args = parser.parse_args()

    sds = SDSClient(services_config['sds'], other_config['timeout'], 'odp_queue', ODPClient())

    if args.debug:
        #query dataset
        dataset_url = 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12'
        product_id = 'DAT-21-en'
        # result_rdf = sds.query_dataset(dataset_url, product_id)
        # dump_rdf('.debug.1.sds.%s.rdf.xml' % product_id, result_rdf)
        # dump_json('.debug.2.sds.%s.json.txt' % product_id, result_json)

        # add to queue
        _rabbit = sds.get_rabbit()
        sds.add_to_queue(_rabbit, 'update', dataset_url, product_id)
        sds.add_to_queue(_rabbit, 'delete', dataset_url, product_id)
        _rabbit.close_connection()

        #query all datasets - UNCOMMENT IF YOU NEED THIS
        #result_json = sds.query_all_datasets()
        #dump_json('.debug.3.sds.all_datasets.json.txt', result_json)
        #dump_rdf('.debug.4.sds.all_datasets.csv.txt', '\n'.join(('\t'.join(x) for x in sds.parse_datasets_json(result_json))))
    else:
        #initiate a bulk update operation
        sds.bulk_update()
