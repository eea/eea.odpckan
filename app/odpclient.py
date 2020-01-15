""" ODP client - use data from SDS in JSON format and update the ODP
    SDS (http://semantic.eea.europa.eu/)
    ODP (https://open-data.europa.eu/en/data/publisher/eea)
"""

import re
import json
from pathlib import Path
from copy import deepcopy

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
    u'license_id': u'cc-by',
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

class ODPClient:
    """ ODP client
    """

    def __init__(self, user_agent=None):
        self.__address = ckan_config['ckan_address']
        self.__apikey = ckan_config['ckan_apikey']
        self.__user_agent = user_agent
        self.__conn = ckanapi.RemoteCKAN(
            self.__address,
            self.__apikey,
            self.__user_agent,
            base_url='apiodp/action/',
        )
        logger.info('Connected to %s' % self.__address)

    def transformJSON2DataPackage(self, dataset_json, dataset_rdf, flg_tags=True):
        """
            refs: http://dataprotocols.org/data-packages/
            :param flg_tags: if set, query ODP for full tag data
        """
        dataset = deepcopy(SKEL_DATASET)
        name = None

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
        license_key = 'http://purl.org/dc/terms/license'
        keyword_key = 'http://open-data.europa.eu/ontologies/ec-odp#keyword'
        identifier_key = 'http://purl.org/dc/terms/identifier'
        publisher_key = 'http://purl.org/dc/terms/publisher'
        issued_key = 'http://purl.org/dc/terms/issued'
        status_key = 'http://open-data.europa.eu/ontologies/ec-odp#datasetStatus'
        modified_key = 'http://purl.org/dc/terms/modified'
        contactname_key = 'http://xmlns.com/foaf/0.1/name'
        contactphone_key = 'http://xmlns.com/foaf/0.1/phone'
        contactaddress_key = 'http://open-data.europa.eu/ontologies/ec-odp#contactAddress'
        contactwebpage_key = 'http://xmlns.com/foaf/0.1/workplaceHomepage'
        isreplacedby_key = u'http://purl.org/dc/terms/isReplacedBy'
        replaces_key = u'http://purl.org/dc/terms/replaces'

        for data in dataset_json:
            if '@type' in data:
                if RESOURCE_TYPE in data['@type']:
                    resource = deepcopy(SKEL_RESOURCE)
                    resource.update({
                        u'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        u'format': data['http://open-data.europa.eu/ontologies/ec-odp#distributionFormat'][0]['@value'],
                        u'url': convert_directlink_to_view(data['http://www.w3.org/ns/dcat#accessURL'][0]['@value']),
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
                    contact_webpage = [
                        d['@id'] for d in data.get(contactwebpage_key, {}) if '@id' in d
                    ]

                    dataset.update({
                        'contact_address': contact_address and contact_address[0] or '',
                        'contact_name': contact_name and contact_name[0] or '',
                        'contact_telephone': contact_phone and contact_phone[0] or '',
                        'contact_webpage': contact_webpage and contact_webpage[0] or '',
                    })

                if DATASET_TYPE in data['@type']:

                    geo_coverage = [
                        d['@id'] for d in data.get(spatial_key, {}) if '@id' in d
                    ]
                    license = [
                        d['@id'] for d in data.get(license_key, {}) if '@id' in d
                    ]
                    keywords = [
                        d['@value'] for d in data.get(keyword_key, {}) if '@value' in d
                    ]
                    identifier = [
                        d['@value'] for d in data.get(identifier_key, {}) if '@value' in d
                    ]
                    publisher = [
                        d['@id'] for d in data.get(publisher_key, {}) if '@id' in d
                    ]
                    issued = [
                        d['@value'] for d in data.get(issued_key, {}) if '@value' in d
                    ]
                    status = [
                        d['@id'] for d in data.get(status_key, {}) if '@id' in d
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
                                        u'display_name': keyword_data[u'name'],
                                        u'id': keyword_data[u'id'],
                                        u'name': keyword_data[u'name'],
                                        u'vocabulary_id': keyword_data[u'vocabulary_id'],
                                    })
                                    dataset[u'tags'].append(keyword_dict)

                    dataset_title = data[u'http://purl.org/dc/terms/title'][0]['@value']
                    dataset_description = data[u'http://purl.org/dc/terms/description'][0]['@value']

                    #if the dataset has no replacement then is the LATEST VERSION
                    if not isreplacedby:
                        dataset_title = u'[LATEST VERSION] %s' % dataset_title
                        dataset_description = u'LATEST VERSION. %s' % dataset_description

                    #emulate versions with additional resources
                    for item in isreplacedby:
                        resource = deepcopy(SKEL_RESOURCE)
                        resource.update({
                            u'description': 'NEWER VERSION',
                            u'format': 'text/html',
                            u'url': item,
                        })
                        dataset[u'resources'].append(resource)

                    for item in replaces:
                        resource = deepcopy(SKEL_RESOURCE)
                        resource.update({
                            u'description': 'OLDER VERSION',
                            u'format': 'text/html',
                            u'url': item,
                        })
                        dataset[u'resources'].append(resource)

                    dataset.update({
                        u'title': dataset_title,
                        u'description': dataset_description,
                        u'url': data['@id'],
                        u'license_id': license and license[0] or "",
                        u'geographical_coverage': geo_coverage,
                        u'identifier': identifier and identifier[0] or "",
                        u'rdf': strip_special_chars(dataset_rdf),
                        u'issued': issued and issued[0] or "",
                        u'publisher': publisher and publisher[0] or "",
                        u'status': status and status or [],
                        u'metadata_modified': modified and modified[0] or "",
                        u'modified_date': modified and modified[0][:10] or "",
                        u'concepts_eurovoc': concepts_eurovoc,
                    })

                    name = [d['@value'] for d in data[u'http://open-data.europa.eu/ontologies/ec-odp#ckan-name'] if '@value' in d]

        dataset[u'num_resources'] = len(dataset[u'resources'])
        dataset[u'owner_org'] = OWNER_ORG

        name = name and name[0] or dataset[u'identifier']

        return name, dataset

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

    def get_ckan_uri(self, dataset_identifier):
        return u"http://data.europa.eu/88u/dataset/" + dataset_identifier

    def render_ckan_rdf(self, ckan_uri, dataset_json):
        """ Render a RDF/XML that the ODP API will accept
        """
        template = jinja_env.get_template('ckan_package.rdf.xml')
        return template.render({
            "uri": ckan_uri,
            "title": "ceva",
            "description": "some description",
        })

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

    def package_delete(self, dataset_identifier):
        """ Delete a package by dataset_identifier
        """
        envelope = {
            "id": dataset_identifier,
        }
        resp = self.__conn.call_action("package_delete", data_dict=envelope)
        # resp_info = resp[0][ckan_uri]
        # assert not resp_info.get("errors")
        return resp

    def call_action(self, action, dataset_identifier, dataset_json):
        """ Call ckan action
        """
        try:
            # name, datapackage = self.transformJSON2DataPackage(dataset_json,
            #                                                    dataset_data_rdf)

            if action in ['update', 'create']:
                ckan_uri = self.get_ckan_uri(dataset_identifier)
                ckan_rdf = self.render_ckan_rdf(ckan_uri, dataset_json)
                self.package_save(ckan_uri, ckan_rdf)

            elif action == 'delete':
                # TODO the API returns internal error; uncomment when it works
                pass # self.package_delete(dataset_identifier)

            else:
                raise RuntimeError("Unknown action %r" % action)

        except Exception, error:
            return ["error", "%s: %s" %(type(error).__name__, error)]

        else:
            return [None, None]
