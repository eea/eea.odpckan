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

    contactPoint = 'http://www.eea.europa.eu/address.html'
    contactPoint_type = 'http://xmlns.com/foaf/0.1/Agent'
    foaf_phone = '+4533367100'
    foaf_name = 'European Environment Agency'
    ecodp_contactAddress = 'Kongens Nytorv 6, 1050 Copenhagen K, Denmark'
    foaf_workplaceHomepage = 'http://www.eea.europa.eu'

    def __init__(self, endpoint, timeout, queue_name, odp):
        """ """
        self.endpoint = endpoint
        self.timeout = timeout
        self.queue_name = queue_name
        self.odp = odp

    def reduce_to_length(self, text, max_length):
        parts = text.split("-")
        reduced_text = ""
        for i in range(1, len(parts) + 1):
            tmp_reduced_text = "-".join(parts[0:i])
            if len(tmp_reduced_text) < max_length:
                reduced_text = tmp_reduced_text
        return reduced_text

    def parse_datasets_json(self, datasets_json):
        """ Parses a response with datasets from SDS in JSON format.
        """
        r = []
        for item_json in datasets_json['results']['bindings']:
            dataset_identifier = item_json['id']['value']
            dataset_url = item_json['dataset']['value']
            r.append((dataset_url, dataset_identifier))
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
        query_url = '%(endpoint)s?%(query)s' % {'endpoint': self.endpoint, 'query': urllib.urlencode(query)}
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        urllib2.install_opener(opener)
        req = urllib2.Request(query_url)
        req.add_header('Accept', content_type)
        try:
            conn = urllib2.urlopen(req, timeout=self.timeout)
        except Exception, err:
            logger.error('SDS connection error: %s', err)
            msg = 'Failure in open'
            conn = None
        if conn:
            result = conn.read()
            conn.close()
            conn = None
        return result, msg

    def query_dataset(self, dataset_url, dataset_identifier):
        """ Given a dataset URL interogates the SDS service
            about it and returns the result which is RDF.
            The RDF result will be converted also to JSON.
        """
        result_rdf, result_json, msg = None, None, ''
        logger.info('START query dataset \'%s\' - \'%s\'',
                    dataset_url, dataset_identifier)
        dataset_ckan_name = "%s_%s" %(dataset_url.split("/")[-2], dataset_identifier)
        dataset_ckan_name = self.reduce_to_length(dataset_ckan_name, 100)
        query = {
            'query': other_config['query_dataset'] % (self.publisher,
                self.datasetStatus,
                self.license,
                dataset_identifier,
                dataset_ckan_name,
                self.contactPoint,
                self.contactPoint_type,
                self.foaf_phone,
                self.foaf_name,
                self.ecodp_contactAddress,
                self.foaf_workplaceHomepage,
                self.odp_license,
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
                                dataset_url, dataset_identifier)
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
            rabbit = RabbitMQConnector(**rabbit_config)
            rabbit.open_connection()
            rabbit.declare_queue(self.queue_name)
            counter = 1
            for item_json in datasets_json:
                dataset_identifier = item_json['id']['value']
                dataset_url = item_json['dataset']['value']
                action = 'update'
                body = '%(action)s|%(dataset_url)s|%(dataset_identifier)s' % {
                    'action': action,
                    'dataset_url': dataset_url,
                    'dataset_identifier': dataset_identifier}
                logger.info('BULK update %s: sending \'%s\' in \'%s\'', counter, body, self.queue_name)
                rabbit.send_message(self.queue_name, body)
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
        dataset_url = 'http://www.eea.europa.eu/themes/biodiversity/document-library/natura-2000/natura-2000-network-statistics/natura-2000-barometer-statistics/statistics/barometer-statistics'
        dataset_identifier = dataset_url.split('/')[-1]
        result_rdf, result_json, msg = sds.query_dataset(dataset_url, dataset_identifier)
        if not msg:
            dump_rdf('.debug.1.sds.%s.rdf.xml' % dataset_identifier, result_rdf)
            dump_json('.debug.2.sds.%s.json.txt' % dataset_identifier, result_json)

        #query all datasets - UNCOMMENT IF YOU NEED THIS
        #result_json, msg = sds.query_all_datasets()
        #if not msg:
        #    dump_json('.debug.3.sds.all_datasets.json.txt', result_json)
        #    dump_rdf('.debug.4.sds.all_datasets.csv.txt', '\n'.join(('\t'.join(x) for x in sds.parse_datasets_json(result_json))))
    else:
        #initiate a bulk update operation
        sds.bulk_update()
