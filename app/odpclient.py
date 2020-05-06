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
import rdflib
import jinja2

from config import logger, ckan_config, services_config, dump_json

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        searchpath=str(Path(__file__).parent / 'templates'),
    ),
    autoescape=jinja2.select_autoescape(['html', 'xml'])
)


RESOURCE_TYPE = u'http://www.w3.org/TR/vocab-dcat#Download'
DATASET_TYPE = u'http://www.w3.org/ns/dcat#Dataset'
CONTACT_TYPE = u'http://xmlns.com/foaf/0.1/Agent'

EUROVOC_PREFIX = u'http://eurovoc.europa.eu/'

SKEL_DATASET = {
    u'title': None,
    u'author': None,
    u'author_email': None,
    u'maintainer': None,
    u'maintainer_email': None,
    u'notes': u'',
    u'url': None,
    u'version': None,
    u'state': u'active',
    u'type': u'dataset',
    u'resources': [],
    u'keywords': [],    #THIS MUST BE SENT EMPTY OTHERWISE WILL RAISE AN ERROR!
    u'tags': []
}
SKEL_RESOURCE = {
    u'description': None,
    u'format': None,
    u'resource_type': RESOURCE_TYPE,
    u'state': u'active',
    u'url': None,
}
SKEL_KEYWORD = {
    u'display_name': None,
    u'id': None,
    u'name': None,
    u'state': u'active',
    u'revision_timestamp': None,
    u'vocabulary_id': None,
}
OWNER_ORG = u'a0f11636-49f9-46ec-9735-c78546d2e9f4'

DATASET_MISSING = 0
DATASET_EXISTS = 1
DATASET_DELETED = 2
DATASET_PRIVATE = 3

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

DAVIZ_TYPE = 'http://www.eea.europa.eu/portal_types/DavizVisualization#DavizVisualization'

DISTRIBUTION_TYPES = {
    'download': 'http://publications.europa.eu/resource/authority/distribution-type/DOWNLOADABLE_FILE',
    'visualization': 'http://publications.europa.eu/resource/authority/distribution-type/VISUALIZATION',
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

    def process_sds_result(self, dataset_rdf, flg_tags=True):
        """
            refs: http://dataprotocols.org/data-packages/
            :param flg_tags: if set, query ODP for full tag data
        """
        g = rdflib.Graph().parse(data=dataset_rdf)
        dataset_json = json.loads(g.serialize(format='json-ld'))

        dataset = deepcopy(SKEL_DATASET)

        def strip_special_chars(s):
            """ return s without special chars
            """
            return re.sub('\s+', ' ', s)

        def convert_directlink_to_view(url):
            """ replace direct links to files to the corresponding web page
            """
            return url.replace('/at_download/file', '/view')

        spatial_key = 'http://purl.org/dc/terms/spatial'
        theme_key = 'http://www.w3.org/ns/dcat#theme'
        keyword_key = 'http://open-data.europa.eu/ontologies/ec-odp#keyword'
        issued_key = 'http://purl.org/dc/terms/issued'
        modified_key = 'http://purl.org/dc/terms/modified'
        contactname_key = 'http://xmlns.com/foaf/0.1/name'
        contactphone_key = 'http://xmlns.com/foaf/0.1/phone'
        contactaddress_key = 'http://open-data.europa.eu/ontologies/ec-odp#contactAddress'
        contacthomepage_key = 'http://xmlns.com/foaf/0.1/workplaceHomepage'
        isreplacedby_key = u'http://purl.org/dc/terms/isReplacedBy'
        replaces_key = u'http://purl.org/dc/terms/replaces'

        for data in dataset_json:
            if '@type' in data:
                if RESOURCE_TYPE in data['@type']:
                    if DAVIZ_TYPE in data['@type']:
                        distribution_type = DISTRIBUTION_TYPES['visualization']
                    else:
                        distribution_type = DISTRIBUTION_TYPES['download']
                    resource = deepcopy(SKEL_RESOURCE)
                    resource.update({
                        u'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        u'format': data['http://open-data.europa.eu/ontologies/ec-odp#distributionFormat'][0]['@value'],
                        u'url': convert_directlink_to_view(data['http://www.w3.org/ns/dcat#accessURL'][0]['@value']),
                        u'distribution_type': distribution_type,
                    })
                    dataset[u'resources'].append(resource)

                if CONTACT_TYPE in data['@type']:
                    contact_name = [
                        d['@value'] for d in data.get(contactname_key, {}) if '@value' in d
                    ]
                    contact_phone = [
                        d['@value'] for d in data.get(contactphone_key, {}) if '@value' in d
                    ]
                    contact_address = [
                        d['@value'] for d in data.get(contactaddress_key, {}) if '@value' in d
                    ]
                    contact_homepage = [
                        d['@id'] for d in data.get(contacthomepage_key, {}) if '@id' in d
                    ]

                    dataset.update({
                        'contact_address': contact_address and contact_address[0] or '',
                        'contact_name': contact_name and contact_name[0] or '',
                        'contact_telephone': contact_phone and contact_phone[0] or '',
                        'contact_homepage': contact_homepage and contact_homepage[0] or '',
                    })

                if DATASET_TYPE in data['@type']:

                    geo_coverage = [
                        d['@id'] for d in data.get(spatial_key, {}) if '@id' in d
                    ]
                    keywords = [
                        d['@value'] for d in data.get(keyword_key, {}) if '@value' in d
                    ]
                    issued = [
                        d['@value'] for d in data.get(issued_key, {}) if '@value' in d
                    ]
                    modified = [
                        d['@value'] for d in data.get(modified_key, {}) if '@value' in d
                    ]
                    concepts_eurovoc = [
                        d['@id'] for d in data.get(theme_key, {}) if '@id' in d and d['@id'].startswith(EUROVOC_PREFIX)
                    ]
                    isreplacedby = [
                        d['@id'] for d in data.get(isreplacedby_key, []) if '@id' in d
                    ]
                    replaces = [
                        d['@id'] for d in data.get(replaces_key, []) if '@id' in d
                    ]

                    #process keywords list
                    if flg_tags:
                        for item in keywords:
                            tag_data = self.tag_search(item)
                            if tag_data[u'count']>0:
                                #keyword found in ODP. iterate the returned list
                                #and identify it.
                                keyword_data = None
                                for d in tag_data[u'results']:
                                    if d[u'name']==item:
                                        keyword_data = d
                                        break
                                if keyword_data is not None:
                                    keyword_dict = deepcopy(SKEL_KEYWORD)
                                    keyword_dict.update({
                                        u'name': keyword_data[u'name'],
                                    })
                                    dataset[u'tags'].append(keyword_dict)

                    dataset_title = data[u'http://purl.org/dc/terms/title'][0]['@value']
                    dataset_description = data[u'http://purl.org/dc/terms/description'][0]['@value']

                    if isreplacedby:
                        raise RuntimeError("Dataset %r is obsolete" % data['@id'])

                    for item in replaces:
                        resource = deepcopy(SKEL_RESOURCE)
                        resource.update({
                            u'description': 'OLDER VERSION',
                            u'format': 'text/html',
                            u'url': item,
                            u'distribution_type': DISTRIBUTION_TYPES['download'],
                        })
                        dataset[u'resources'].append(resource)

                    dataset.update({
                        u'title': dataset_title,
                        u'description': dataset_description,
                        u'url': data['@id'],
                        u'geographical_coverage': geo_coverage,
                        u'issued': issued and issued[0] or "",
                        u'metadata_modified': modified and modified[0] or "",
                        u'concepts_eurovoc': concepts_eurovoc,
                    })

        dataset[u'num_resources'] = len(dataset[u'resources'])
        dataset[u'owner_org'] = OWNER_ORG

        return dataset

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

    def render_ckan_rdf(self, ckan_uri, product_id, dataset_rdf):
        """ Render a RDF/XML that the ODP API will accept
        """
        template = jinja_env.get_template('ckan_package.rdf.xml')
        context = self.process_sds_result(dataset_rdf)
        for resource in context.get('resources', []):
            resource['_uuid'] = str(uuid.uuid4())
            resource['filetype'] = (
                "http://publications.europa.eu/resource/authority/file-type/"
                + FILE_TYPES.get(resource.get('format'), 'OCTET')
            )
        context.update({
            "uri": ckan_uri,
            "product_id": product_id,
            "landing_page": re.sub(r'^http://', 'https://', context['url']),
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

    def call_action(self, action, product_id, dataset_rdf):
        """ Call ckan action
        """
        try:
            if action in ['update', 'create']:
                ckan_uri = self.get_ckan_uri(product_id)
                ckan_rdf = self.render_ckan_rdf(ckan_uri, product_id, dataset_rdf)
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
