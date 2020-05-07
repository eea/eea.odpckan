""" ODP client - use data from SDS in JSON format and update the ODP
    SDS (http://semantic.eea.europa.eu/)
    ODP (https://open-data.europa.eu/en/data/publisher/eea)
"""

import re
import json
from pathlib import Path
from copy import deepcopy
import uuid

import ckanapi
from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import DCTERMS, XSD, FOAF, RDF
import jinja2

from config import logger, ckan_config, services_config, dump_json

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


jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        searchpath=str(Path(__file__).parent / 'templates'),
    ),
    autoescape=jinja2.select_autoescape(['html', 'xml'])
)


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


class ODPClient:
    """ ODP client
    """

    def __init__(self, user_agent=None):
        self.__address = ckan_config['ckan_address']
        self.__apikey = ckan_config['ckan_apikey']
        self.__user_agent = user_agent

        if ckan_config['ckan_proxy']:
            import requests
            session = requests.Session()
            session.proxies = {
                'http': ckan_config['ckan_proxy'],
                'https': ckan_config['ckan_proxy'],
            }
        else:
            session = None

        self.__conn = ckanapi.RemoteCKAN(
            self.__address,
            self.__apikey,
            self.__user_agent,
            base_url='apiodp/action/',
            session=session,
        )
        logger.info('Connected to %s' % self.__address)

    def process_sds_result(self, dataset_rdf, dataset_url):
        """
            refs: http://dataprotocols.org/data-packages/
        """
        g = Graph().parse(data=dataset_rdf)
        dataset = URIRef(dataset_url)

        if g.value(dataset, DCTERMS.isReplacedBy) is not None:
            raise RuntimeError("Unknown action %r" % action)

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

    def tag_search(self, tag_name):
        """ Get the tag by name. It returns a dictionary like:
            {u'count': 1, u'results': [{u'vocabulary_id': None, u'id': u'tag_id', u'name': u'tag_name'}]}
        """
        resp = None
        try:
            resp = self.__conn.action.tag_search(query=tag_name)
        except ckanapi.NotFound:
            logger.error('Search tag: \'%s\' not found.' % tag_name)
        else:
            if resp[u'count']==0:
                logger.error('Search tag: \'%s\' not found.' % tag_name)
            else:
                logger.info('Search tag: \'%s\' found.' % tag_name)
        return resp

    def get_ckan_uri(self, product_id):
        return u"http://data.europa.eu/88u/dataset/" + product_id

    def render_ckan_rdf(self, ckan_uri, product_id, dataset_rdf, dataset_url):
        """ Render a RDF/XML that the ODP API will accept
        """
        template = jinja_env.get_template('ckan_package.rdf.xml')
        context = self.process_sds_result(dataset_rdf, dataset_url)
        for resource in context.get('resources', []):
            resource['_uuid'] = str(uuid.uuid4())
        context.update({
            "uri": ckan_uri,
            "product_id": product_id,
            "uuids": {
                "landing_page": str(uuid.uuid4()),
                "contact": str(uuid.uuid4()),
                "contact_homepage": str(uuid.uuid4()),
                "contact_telephone": str(uuid.uuid4()),
                "contact_address": str(uuid.uuid4()),
            }
        })
        return template.render(context)

    def package_save(self, ckan_uri, ckan_rdf):
        """ Save a package
        """
        envelope = {
            "addReplaces": [{
                "objectUri": ckan_uri,
                "addReplace": {"objectStatus": "published"},
            }],
            "rdfFile": ckan_rdf,
        }
        resp = self.__conn.call_action("package_save", data_dict=envelope)
        # resp_info = resp[0][ckan_uri]
        # assert not resp_info.get("errors")
        # assert resp_info.get("status_save")
        return resp

    def package_delete(self, product_id):
        """ Delete a package by product_id
        """
        envelope = {
            "id": product_id,
        }
        resp = self.__conn.call_action("package_delete", data_dict=envelope)
        # resp_info = resp[0][ckan_uri]
        # assert not resp_info.get("errors")
        return resp

    def call_action(self, action, product_id, dataset_rdf, dataset_url):
        """ Call ckan action
        """
        try:
            if action in ['update', 'create']:
                ckan_uri = self.get_ckan_uri(product_id)
                ckan_rdf = self.render_ckan_rdf(ckan_uri, product_id, dataset_rdf, dataset_url)
                self.package_save(ckan_uri, ckan_rdf)

            elif action == 'delete':
                # TODO the API returns internal error; uncomment when it works
                pass # self.package_delete(product_id)

            else:
                raise RuntimeError("Unknown action %r" % action)

        except Exception, error:
            return ["error", "%s: %s" %(type(error).__name__, error)]

        else:
            return [None, None]
