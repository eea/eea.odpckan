""" ODP client - use data from SDS in JSON format and update the ODP
    SDS (http://semantic.eea.europa.eu/)
    ODP (https://open-data.europa.eu/en/data/publisher/eea)

"""

import ckanapi
import pprint
import re

from config import logger, ckan_config, services_config

RESOURCE_TYPE = 'http://www.w3.org/TR/vocab-dcat#Download'
DATASET_TYPE = 'http://www.w3.org/ns/dcat#Dataset'

SKEL_DATASET = {
    'title': None,
    'author': None,
    'author_email': None,
    'maintainer': None,
    'maintainer_email': None,
    'license_id': 'cc-by',
    'notes': '',
    'url': None,
    'version': None,
    'state': 'active',
    'type': 'dataset',
    'resources': [],
    'keywords': [],
}
SKEL_RESOURCE = {
    "description": None,
    "format": None,
    "resource_type": RESOURCE_TYPE,
    "state": "active",
    "url": None,
}
OWNER_ORG = "a0f11636-49f9-46ec-9735-c78546d2e9f4"


class ODPClient:
    """ ODP client
    """
        
    def __init__(self, user_agent=None):
        self.__address = ckan_config['ckan_address']
        self.__apikey = ckan_config['ckan_apikey']
        self.__user_agent = user_agent
        self.__conn = ckanapi.RemoteCKAN(self.__address,
            self.__apikey,
            self.__user_agent)
        logger.info('Connect to %s' % self.__address)
    
    def __dump(self, fname, value):
        """
        """
        f = open(fname, 'w')
        pprint.pprint(value, f)
        f.close()
        
    
    def transformJSON2DataPackage(self, dataset_json, dataset_rdf):
        """
            refs: http://dataprotocols.org/data-packages/
        """
        dataset = SKEL_DATASET.copy()
        name = None
        
        def strip_special_chars(s):
            """ return s without special chars
            """
            return re.sub('\s+', ' ', s)
        
        spatial_key = 'http://purl.org/dc/terms/spatial'
        license_key = 'http://purl.org/dc/terms/license'
        keyword_key = 'http://open-data.europa.eu/ontologies/ec-odp#keyword'
        identifier_key = 'http://purl.org/dc/terms/identifier'
        
        for data in dataset_json:
            if '@type' in data:
                if RESOURCE_TYPE in data['@type']:
                    resource = SKEL_RESOURCE.copy()
                    resource.update({
                        'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        'format': data['http://open-data.europa.eu/ontologies/ec-odp#distributionFormat'][0]['@value'],
                        'url': data['http://www.w3.org/ns/dcat#accessURL'][0]['@value'],
                    })
                    dataset['resources'].append(resource)
        
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
                    
                    dataset.update({
                        'title': data['http://purl.org/dc/terms/title'][0]['@value'],
                        'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        'url': data['@id'],
                        'license_id': license and license[0] or "",
                        'geographical_coverage': geo_coverage,
                        'identifier': identifier and identifier[0] or "",
                        'keywords': keywords,
                        'rdf': strip_special_chars(dataset_rdf),
                    })
                    
                    name = [d['@value'] for d in data['http://open-data.europa.eu/ontologies/ec-odp#ckan-name'] if '@value' in d]

        dataset['num_resources'] = len(dataset['resources'])
        dataset['owner_org'] = OWNER_ORG
        
        name = name and name[0] or dataset['identifier']
        
        return name, dataset
        
    def package_show(self, package_name):
        """ Get the package by name
        """
        resp = None
        try:
            resp = self.__conn.action.package_show(id=package_name)
        except ckanapi.NotFound:
            logger.error('Package \'%s\' not found.' % package_name)
        else:
            logger.info('Package \'%s\' found.' % package_name)
        return resp
        
    def package_search(self, prop, value):
        """ Search for a package
        """
        resp = None
        try:
            resp = self.__conn.action.package_search(fq='%s:%s' % (prop, value))
        except Exception, error:
            logger.error('Query for package \'%s:%s\'.' % (prop, value))
        else:
            logger.info('Query for package \'%s:%s\'. Found %s.' % (
                    prop, value, resp[u'count']
            ))
            resp = resp[u'results']
        return resp
        

    def package_create(self, data_package):
        """ Create a package
        """
        msg = ''
        package_title = data_package['title']
        resp = self.package_search(
            prop='identifier', value=package_title
        )
        
        if resp == []:
            try:
                resp = self.__conn.call_action("package_create",
                    data_dict=data_package)
            except Exception, error:
                msg = 'Got an error to execute the command for %s.: %s' % (
                    package_title, error
                )
                logger.error(msg)
            else:
                logger.info('Package \'%s\' added.' % resp[u'name'])
        else:
            msg = 'Package \'%s\' not found.' % package_title
            logger.info(msg)
            
        return resp, msg

    def package_update(self, data_package):
        """ Update an existing package
        """
        msg = ''
        package_title = data_package['title']
        resp = self.package_search(
            prop='title', value=package_title
        )
        
        if resp:
            package = resp[0]
            package.update(data_package)
            try:
                resp = self.__conn.call_action(
                    'package_update', data_dict=package, apikey=self.__apikey)
            except ckanapi.NotFound:
                msg = 'Package \'%s\' not found.' % package_title
                logger.info(msg)
                return False, msg
            except Exception, error:
                msg = 'Got an error to execute the command for %s.: %s' % (
                    package_title, error
                )
                logger.error(msg)
                return False, msg
            else:
                logger.info('Package \'%s\' updated.' % resp[u'title'])
        else:
            msg = 'Package \'%s\' not found.' % package_title
            logger.info(msg)
            
        return resp, msg
    
    def package_delete(self, package_title):
        """ Delete a package by package_title
            return True if the operation success
                   False otherwise
        """
        msg = ''
        
        resp = self.package_search(
            prop='title', value=package_title
        )
        
        if not resp:
            msg = 'Package \'%s\' not found.' % package_title
            logger.error(msg)
            return False, msg
        
        package_id = resp[0]['id']
        
        try:
            resp = self.__conn.action.package_delete(id=package_id)
        except ckanapi.NotFound:
            msg = 'Package \'%s\' not found.' % package_title
            logger.info(msg)
            return False, msg
        except Exception, error:
            msg = 'Got an error to execute the command for %s.: %s' % (
                package_title, error
            )
            logger.error(msg)
            return False, msg
        else:
            logger.info('Package \'%s\' deleted.' % package_title)
        
        return True, msg
        
        
    def resource_show(self, resource_name):
        """ Get the resource by name
        """
        resp = None
        try:
            resp = self.__conn.action.resource_show(id=resource_name)
        except ckanapi.NotFound:
            logger.error('Resource \'%s\' not found.' % resource_name)
        else:
            logger.info('Resource \'%s\' found.' % package_name)
        return resp

    def resource_create(self, package_id, resource_name, url,
        description=u'', resource_type=u'', state=u'active'):
        """ Create a resource
        """
        resp = self.resource_show(package_id)
        if resp is None:
            resp = self.__conn.action.resource_create(package_id=package_id,
                name=resource_name, url=url, description=description,
                resource_type=resource_type, state=state)
            logger.info('Resource \'%s\' added.' % resource_name)
        return resp
    
    def call_action(self, action, dataset_json={}, dataset_data_rdf=None):
        """ Call ckan action
        """
        
        name, datapackage = self.transformJSON2DataPackage(dataset_json, dataset_data_rdf)
        
        if action in ['update', 'create']:
            if action == 'create':
                datapackage['name'] = name
                return self.package_create(datapackage)
            else:
                return self.package_update(datapackage)
        
        if action == 'delete':
            package_title = datapackage['title']
            return self.package_delete(package_title)

if __name__ == '__main__':
    
    #query dataset
    odp = ODPClient()
    package = odp.package_show(u'XsJfLAZ4guXeAL4bjHNA')
    package = odp.package_search(prop='name', value=u'FPi519FhZ8UHVCNmdjhqPg')
    package = odp.package_search(prop='identifier', value=u'biogeographical-regions-europes')
