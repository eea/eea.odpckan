""" SDS client - methods to query/get a dataset data from
    SDS (http://semantic.eea.europa.eu/) 
"""

import sys
import argparse
import urllib, urllib2
import rdflib
import json
import re

from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import DCTERMS, XSD, FOAF, RDF
from eea.rabbitmq.client import RabbitMQConnector

from config import logger, dump_rdf, dump_json
from config import services_config, rabbit_config, other_config
from odpclient import ODPClient

DCAT = Namespace(u'http://www.w3.org/ns/dcat#')
VCARD = Namespace(u'http://www.w3.org/2006/vcard/ns#')
ADMS = Namespace(u'http://www.w3.org/ns/adms#')
SCHEMA = Namespace(u'http://schema.org/')
EU_FILE_TYPE = Namespace(u'http://publications.europa.eu/resource/authority/file-type/')
EU_DISTRIBUTION_TYPE = Namespace(u'http://publications.europa.eu/resource/authority/distribution-type/')
EU_LICENSE = Namespace(u'http://publications.europa.eu/resource/authority/licence/')
EU_STATUS = Namespace(u'http://publications.europa.eu/resource/authority/dataset-status/')
EU_COUNTRY = Namespace(u'http://publications.europa.eu/resource/authority/country/')
EUROVOC = Namespace(u'http://eurovoc.europa.eu/')
ECODP = Namespace(u'http://open-data.europa.eu/ontologies/ec-odp#')
DAVIZ = Namespace(u'http://www.eea.europa.eu/portal_types/DavizVisualization#')


FILE_TYPES = {
    'application/msaccess': 'MDB',
    'application/msword': 'DOC',
    'application/octet-stream': 'OCTET',
    'application/pdf': 'PDF',
    'application/vnd.google-earth.kml+xml': 'KML',
    'application/vnd.ms-excel': 'XLS',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'XLSX',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'DOCX',
    'application/x-dbase': 'DBF',
    'application/x-e00': 'E00',
    'application/xml': 'XML',
    'application/zip': 'ZIP',
    'image/gif': 'GIF',
    'image/jpeg': 'JPEG',
    'image/png': 'PNG',
    'image/tiff': 'TIFF',
    'text/comma-separated-values': 'CSV',
    'text/csv': 'CSV',
    'text/html': 'HTML',
    'text/plain': 'TXT',
    'text/xml': 'XML',
}

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

    def query_dataset(self, dataset_url):
        """ Given a dataset URL interogates the SDS service
            about it and returns the result which is RDF.
        """
        logger.info('query dataset \'%s\'', dataset_url)
        query = other_config['query_dataset'] % {"dataset": dataset_url}
        return self.query_sds(query, 'application/xml')

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

    def parse_dataset(self, dataset_rdf, dataset_url):
        """
            refs: http://dataprotocols.org/data-packages/
        """
        g = Graph().parse(data=dataset_rdf)
        dataset = URIRef(dataset_url)

        if g.value(dataset, DCTERMS.isReplacedBy) is not None:
            raise RuntimeError("Dataset %r is obsolete" % dataset_url)

        def https_link(url):
            return re.sub(r"^http://", "https://", url)

        def convert_directlink_to_view(url):
            """ replace direct links to files to the corresponding web page
            """
            return https_link(url.replace('/at_download/file', '/view'))

        def file_type(mime_type):
            name = FILE_TYPES.get(mime_type, 'OCTET')
            return EU_FILE_TYPE[name]

        EUROVOC_PREFIX = u'http://eurovoc.europa.eu/'

        keywords = [unicode(k) for k in g.objects(dataset, ECODP.keyword)]
        geo_coverage = [unicode(k) for k in g.objects(dataset, DCTERMS.spatial)]
        concepts_eurovoc = [
            unicode(k) for k in g.objects(dataset, DCAT.theme)
            if unicode(k).startswith(EUROVOC_PREFIX)
        ]

        resources = []

        for res in g.objects(dataset, DCAT.distribution):
            types = list(g.objects(res, RDF.type))

            if DAVIZ.DavizVisualization in types:
                distribution_type = EU_DISTRIBUTION_TYPE.VISUALIZATION

            elif URIRef("http://www.w3.org/TR/vocab-dcat#Download") in types:
                distribution_type = EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE

            else:
                raise RuntimeError("Unknown distribution type %r", res)

            resources.append({
                "description": unicode(g.value(res, DCTERMS.description)),
                "filetype": file_type(unicode(g.value(res, ECODP.distributionFormat))),
                "url": convert_directlink_to_view(unicode(g.value(res, DCAT.accessURL))),
                "distribution_type": distribution_type,
            })

        for old in g.objects(dataset, DCTERMS.replaces):
            issued = g.value(old, DCTERMS.issued).toPython().date()
            resources.append({
                "description": u"OLDER VERSION - %s" % issued,
                "filetype": file_type("text/html"),
                "url": https_link(unicode(old)),
                "distribution_type": EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE,
            })

        return {
            "title": unicode(g.value(dataset, DCTERMS.title)),
            "description": unicode(g.value(dataset, DCTERMS.description)),
            "landing_page": https_link(dataset_url),
            "issued": unicode(g.value(dataset, DCTERMS.issued)),
            "metadata_modified": unicode(g.value(dataset, DCTERMS.modified)),
            "keywords": keywords,
            "geographical_coverage": geo_coverage,
            "concepts_eurovoc": concepts_eurovoc,
            "resources": resources,
        }

    def get_dataset(self, dataset_url):
        dataset_rdf = self.query_dataset(dataset_url)
        return self.parse_dataset(dataset_rdf, dataset_url)


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
