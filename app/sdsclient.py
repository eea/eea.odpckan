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

    license = 'http://opendatacommons.org/licenses/by/'
    odp_license = 'http://open-data.europa.eu/kos/licence/EuropeanCommission'
    publisher = 'http://publications.europa.eu/resource/authority/corporate-body/EEA'
    datasetStatus = 'http://data.europa.eu/euodp/kos/dataset-status/Completed'

    contactPoint = 'https://www.eea.europa.eu/address.html'
    contactPoint_type = 'http://xmlns.com/foaf/0.1/Agent'
    foaf_phone = '+4533367100'
    foaf_name = 'European Environment Agency'
    ecodp_contactAddress = 'Kongens Nytorv 6, 1050 Copenhagen K, Denmark'
    foaf_workplaceHomepage = 'https://www.eea.europa.eu'

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

    def reduce_to_length(self, text, max_length):
        """ Reduce the length of text to the max_length without spliting words,
            and if the last word of text is a number, include the number at
            the end of the reduced text
        """
        parts = text.split("-")
        lastWordIsInt = self.is_int(parts[-1])
        if lastWordIsInt:
            max_length = max_length - len(parts[-1]) - 1
            range_max = len(parts)
        else:
            range_max = len(parts) + 1

        reduced_text = ""
        for i in range(1, range_max):
            tmp_reduced_text = "-".join(parts[0:i])
            if len(tmp_reduced_text) < max_length:
                reduced_text = tmp_reduced_text

        if lastWordIsInt:
            reduced_text = "%s-%s" %(reduced_text, parts[-1])

        return reduced_text

    def parse_datasets_json(self, datasets_json):
        """ Parses a response with datasets from SDS in JSON format.
        """
        r = []
        for item_json in datasets_json['results']['bindings']:
            product_id = item_json['product_id']['value']
            dataset_url = item_json['dataset']['value']
            r.append((dataset_url, product_id))
        return r

    def validate_result(self, dataset_json, dataset_rdf):
        """ Validates that the given dataset result is complete.
            We are gone make we of the ODPClient's method
        """
        msg = ''
        try:
            #pass flg_tags = False because we don't want to query ODP for tags right now
            #we just want to make sure that the method builds the package
            self.odp.transformJSON2DataPackage(dataset_json, dataset_rdf, flg_tags=False)
        except Exception, err:
            msg = err
        return msg

    def query_sds(self, query, content_type):
        """ Generic method to query SDS to be used all around.
        """
        result, msg = None, ''
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        urllib2.install_opener(opener)
        data = urllib.urlencode(query)
        req = urllib2.Request(self.endpoint, data=data)
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        req.add_header('Accept', content_type)
        try:
            conn = urllib2.urlopen(req, timeout=self.timeout)
        except Exception, err:
            logger.error('SDS connection error: %s', err)
            if err.url != self.endpoint:
                logger.error('Received redirect from SDS: %s to %s',
                             self.endpoint, err.url)
            msg = 'Failure in open'
            conn = None
        if conn:
            result = conn.read()
            conn.close()
            conn = None
        return result, msg

    def query_dataset(self, dataset_url, product_id):
        """ Given a dataset URL interogates the SDS service
            about it and returns the result which is RDF.
            The RDF result will be converted also to JSON.
        """
        result_rdf, result_json, msg = None, None, ''
        logger.info('START query dataset \'%s\' - \'%s\'',
                    dataset_url, product_id)
        dataset_ckan_name = "%s_%s" %(dataset_url.split("/")[-2], product_id)
        dataset_ckan_name = self.reduce_to_length(dataset_ckan_name, 100)
        query = {
            'query': other_config['query_dataset'] % (self.publisher,
                self.datasetStatus,
                self.license,
                product_id,  # TODO remove?
                dataset_ckan_name,  # TODO remove?
                self.contactPoint,
                self.contactPoint_type,
                self.foaf_phone,
                self.foaf_name,
                self.ecodp_contactAddress,
                self.foaf_workplaceHomepage,
                self.odp_license,
                dataset_url,
                dataset_url,
                dataset_url),
            'format': 'application/xml'
        }
        result_rdf, msg = self.query_sds(query, 'application/xml')
        if msg:
            logger.error('QUERY dataset \'%s\': %s', dataset_url, msg)
        else:
            #convert the RDF to JSON
            try:
                g = rdflib.Graph().parse(data=result_rdf)
                s = g.serialize(format='json-ld')
                #s is a string containg a JSON like structure
                result_json = json.loads(s)
            except Exception, err:
                logger.error('JSON CONVERSION error: %s', err)
                logger.info('ERROR query dataset')
            else:
                #due to this kind of problem 72772#note-38
                #we must validate the data for some requeired fields
                msg = self.validate_result(result_json, result_rdf)
                if msg:
                    logger.error('MISSING DATA error: %s', msg)
                    logger.info('ERROR query dataset')
                else:
                    logger.info('DONE query dataset \'%s\' - \'%s\'',
                                dataset_url, product_id)
        return result_rdf, result_json, msg

    def query_all_datasets(self):
        """ Find all datasets (to pe updated in ODP) in the repository.
        """
        result_json, msg = None, ''
        logger.info('START query all datasets')
        query = {'query': other_config['query_all_datasets'],
                 'format': 'application/json'}
        result, msg = self.query_sds(query, 'application/json')
        if msg:
            logger.error('QUERY all datasets: %s', msg)
        else:
            try:
                result_json = json.loads(result)
            except Exception, err:
                logger.error('JSON CONVERSION error: %s', err)
            else:
                logger.info('DONE query all datasets')
        return result_json, msg

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
        result_json, msg = self.query_all_datasets()
        if msg:
            logger.error('BULK update: %s', msg)
        else:
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
        # result_rdf, result_json, msg = sds.query_dataset(dataset_url, product_id)
        # if not msg:
        #     dump_rdf('.debug.1.sds.%s.rdf.xml' % product_id, result_rdf)
        #     dump_json('.debug.2.sds.%s.json.txt' % product_id, result_json)

        # add to queue
        _rabbit = sds.get_rabbit()
        sds.add_to_queue(_rabbit, 'update', dataset_url, product_id)
        sds.add_to_queue(_rabbit, 'delete', dataset_url, product_id)
        _rabbit.close_connection()

        #query all datasets - UNCOMMENT IF YOU NEED THIS
        #result_json, msg = sds.query_all_datasets()
        #if not msg:
        #    dump_json('.debug.3.sds.all_datasets.json.txt', result_json)
        #    dump_rdf('.debug.4.sds.all_datasets.csv.txt', '\n'.join(('\t'.join(x) for x in sds.parse_datasets_json(result_json))))
    else:
        #initiate a bulk update operation
        sds.bulk_update()
