""" SDS client - methods to query/get a dataset data from
    SDS (http://semantic.eea.europa.eu/) 
"""

import sys
import argparse
import urllib, urllib2
import rdflib
import json

from config import logger, dump_rdf, dump_json
from config import services_config, rabbit_config, other_config
from eea.rabbitmq.client import RabbitMQConnector

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

    def __init__(self, endpoint, timeout, queue_name):
        """ """
        self.endpoint = endpoint
        self.timeout = timeout
        self.queue_name = queue_name

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
            'query': self.datasetQuery % (self.publisher,
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
                logger.info('DONE query dataset \'%s\' - \'%s\'',
                            dataset_url, dataset_identifier)
        return result_rdf, result_json, msg

    datasetQuery = """
PREFIX a: <http://www.eea.europa.eu/portal_types/Data#>
PREFIX dt: <http://www.eea.europa.eu/portal_types/DataTable#>
PREFIX org: <http://www.eea.europa.eu/portal_types/Organisation#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ecodp: <http://open-data.europa.eu/ontologies/ec-odp#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX datafilelink: <http://www.eea.europa.eu/portal_types/DataFileLink#>
PREFIX datafile: <http://www.eea.europa.eu/portal_types/DataFile#>
PREFIX sparql: <http://www.eea.europa.eu/portal_types/Sparql#>
PREFIX file: <http://www.eea.europa.eu/portal_types/File#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX cr: <http://cr.eionet.europa.eu/ontologies/contreg.rdf#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {
 ?dataset a dcat:Dataset;
       dct:publisher <%s>;
       ecodp:datasetStatus <%s>;
       dct:license <%s>;
       dct:license ?odp_license;
       dct:title ?title;
       dct:description ?description;
       dct:identifier '%s';
       ecodp:ckan-name '%s';
       dct:issued ?effective;
       dct:modified ?modified;
       ecodp:keyword ?theme;
       dct:spatial ?pubspatial;
       dct:subject ?subject;
       dcat:theme ?dcat_theme;
       dct:isReplacedBy ?isreplaced;
       dct:replaces ?replaces;

       ecodp:contactPoint ?ecodp_contactPoint .
       ?ecodp_contactPoint  rdf:type ?ecodp_contactPoint_type .
       ?ecodp_contactPoint  foaf:phone ?foaf_phone .
       ?ecodp_contactPoint  foaf:name  ?foaf_name .
       ?ecodp_contactPoint  ecodp:contactAddress ?ecodp_contactAddres .
       ?ecodp_contactPoint foaf:workplaceHomepage ?foaf_workplaceHomepage.

 ?dataset dcat:distribution ?datafile .
 ?datafile dcat:accessURL ?downloadUrl.
 ?datafile a <http://www.w3.org/TR/vocab-dcat#Download>;
       ecodp:distributionFormat ?format;
       dct:description ?dftitle;
       dct:modified ?dfmodified
}
WHERE {
  {
   ?dataset a a:Data ;
        a:id ?id;
        dct:title ?title;
        dct:description ?description;
        dct:hasPart ?datatable.
   OPTIONAL { ?dataset dct:issued ?effective }
   OPTIONAL { ?dataset dct:modified ?modified }
   OPTIONAL { ?dataset dct:isReplacedBy ?isreplaced }
   OPTIONAL { ?dataset dct:replaces ?replaces }
   
   {select (<%s> as ?ecodp_contactPoint) where {}}
   {select (<%s> as ?ecodp_contactPoint_type) where {}}
   {select ("%s" as ?foaf_phone) where {}}
   {select ("%s"@en as ?foaf_name) where {}}
   {select ("%s"@en as ?ecodp_contactAddres) where {}}
   {select (<%s> as ?foaf_workplaceHomepage) where {}}

   {select (STRDT("%s", skos:Concept) as ?odp_license) where {}}
   ?datatable dct:hasPart ?datafile.
   {
     {
       SELECT DISTINCT ?datafile STRDT(bif:concat(?datafile,'/at_download/file'), xsd:anyURI) AS ?downloadUrl ?format
       WHERE {
         ?datafile a datafile:DataFile;
                   dct:format ?format
         filter(str(?format) = "application/zip")
       }
     }
   } UNION {
     {
       SELECT DISTINCT ?datafile STRDT(bif:concat(?datafile,'/at_download/file'), xsd:anyURI) AS ?downloadUrl ?format
       WHERE
       {
         { SELECT DISTINCT ?datafile count(?format) as ?formatcnt
           WHERE {
             ?datafile a datafile:DataFile;
             dct:format ?format
             FILTER (str(?format) != 'application/zip')
           }
         } . FILTER (?formatcnt = 1)
         ?datafile dct:format ?format
       }
     }
   } UNION {
     {
       SELECT DISTINCT ?datafile STRDT(?remoteUrl, xsd:anyURI) AS ?downloadUrl 'application/octet-stream' AS ?format
       WHERE {
         ?datafile a datafilelink:DataFileLink;
                   datafilelink:remoteUrl ?remoteUrl
       }
     }
   } UNION {
     {
       SELECT DISTINCT ?datafile STRDT(bif:concat(?datafile,'/download.csv'), xsd:anyURI) AS ?downloadUrl 'text/csv' as ?format
       WHERE {
         ?datafile a sparql:Sparql
       }
     }
   } UNION {
     {
       SELECT DISTINCT ?datafile STRDT(?datafile, xsd:anyURI) AS ?downloadUrl "file" as ?format
       WHERE {
         ?datafile a file:File
       }
     }
   }
   ?datafile dct:title    ?dftitle .
   ?datafile dct:modified ?dfmodified
  } UNION {
   ?dataset dct:subject ?subject
  } UNION {
   ?dataset dct:subject ?theme  FILTER (isLiteral(?theme) && !REGEX(?theme,'[()/]'))
  } UNION {
   ?dataset dct:spatial ?spatial .
   ?spatial owl:sameAs ?pubspatial
        FILTER(REGEX(?pubspatial, '^http://publications.europa.eu/resource/authority/country/'))
  } UNION {
    ?dataset cr:tag ?tag.
      ?dcat_theme a skos:Concept.
      ?dcat_theme rdfs:label ?tag.
  }
  FILTER (?dataset = <%s> )
}
"""

    def query_all_datasets(self):
        """ Find all datasets (to pe updated in ODP) in the repository.
        """
        result_json, msg = None, ''
        logger.info('START query all datasets')
        query = {'query': self.allDatasetsQuery,
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

    allDatasetsQuery = """
PREFIX a: <http://www.eea.europa.eu/portal_types/Data#>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT DISTINCT ?dataset ?id
WHERE {
  ?dataset a a:Data ;
        a:id ?id;
        dct:description ?description;
        dct:hasPart ?datatable.
  OPTIONAL {?dataset dct:isReplacedBy ?isreplaced }
  ?datatable dct:hasPart ?datafile
}
"""

    def query_obsolete_datasets(self):
        """ Find obsolete (to be removed from ODP) datasets in the repository.
        """
        result_json, msg = None, ''
        logger.info('START query obsolete datasets')
        query = {'query': self.obsoleteDatasetsQuery,
                 'format': 'application/json'}
        result, msg = self.query_sds(query, 'application/json')
        if msg:
            logger.error('QUERY obsolete datasets: %s', msg)
        else:
            try:
                result_json = json.loads(result)
            except Exception, err:
                logger.error('JSON CONVERSION error: %s', err)
                logger.info('ERROR query obsolete datasets')
            else:
                logger.info('DONE query obsolete datasets')
        return result_json, msg

    obsoleteDatasetsQuery = """
PREFIX a: <http://www.eea.europa.eu/portal_types/Data#>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT DISTINCT ?dataset ?id
WHERE {
  ?dataset a a:Data ;
        a:id ?id;
        dct:description ?description;
        dct:hasPart ?datatable.
  ?dataset dct:isReplacedBy ?isreplaced.
  ?datatable dct:hasPart ?datafile.
}
"""

    def bulk_update(self):
        """ Queries SDS for all datasets and injects messages in rabbitmq.
        """
        logger.info('START bulk update')
        #query all datasets
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

    sds = SDSClient(services_config['sds'], other_config['timeout'], 'odp_queue')

    if args.debug:
        #query dataset
        dataset_url = 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-eu-ets-data-from-citl-8'
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

        #query obsolete datasets - UNCOMMENT IF YOU NEED THIS
        #result_json, msg = sds.query_obsolete_datasets()
        #if not msg:
        #    dump_json('.debug.5.sds.obsolete_datasets.json.txt', result_json)
        #    dump_rdf('.debug.6.sds.obsolete_datasets.csv.txt', '\n'.join(('\t'.join(x) for x in sds.parse_datasets_json(result_json))))
    else:
        #initiate a bulk update operation
        sds.bulk_update()
