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
    "revision_timestamp":None,
    "description": None,
    "format": None,
    "resource_type": RESOURCE_TYPE,
    "state": "active",
    "url": None,
}

SKEL_KEYWORD = {
    "display_name": "",
    "name": "",
    "state": "active",
    "vocabulary_id": None
}
        

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
        
        def keyword_dict(keywords):
            res = []
            for k in keywords:
                temp = SKEL_KEYWORD.copy()
                temp.update({
                    'name': k,
                    'display_name': k
                })
                res.append(temp)
                
            return res
        
        def strip_special_chars(s):
            """ return s without special chars
            """
            return re.sub('\s+', ' ', s)
            
        for data in dataset_json:
            if '@type' in data:
                if RESOURCE_TYPE in data['@type']:
                    resource = SKEL_RESOURCE.copy()
                    resource.update({
                        'revision_timestamp': data['http://purl.org/dc/terms/modified'][0]['@value'],
                        'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        'format': data['http://open-data.europa.eu/ontologies/ec-odp#distributionFormat'][0]['@value'],
                        'url': data['http://www.w3.org/ns/dcat#accessURL'][0]['@value'],
                    })
                    dataset['resources'].append(resource)
                    
                if DATASET_TYPE in data['@type']:
                    geo_coverage = [d['@id'] for d in data['http://purl.org/dc/terms/spatial'] if '@id' in d]
                    license = [d['@id'] for d in data['http://purl.org/dc/terms/license'] if '@id' in d]
                    keywords = [d['@value'] for d in data['http://open-data.europa.eu/ontologies/ec-odp#keyword'] if '@value' in d]
                    identifier = [d['@value'] for d in data['http://purl.org/dc/terms/identifier'] if '@value' in d]
                    
                    dataset.update({
                        'title': data['http://purl.org/dc/terms/title'][0]['@value'],
                        'description': data['http://purl.org/dc/terms/description'][0]['@value'],
                        'url': data['@id'],
                        'license_id': license,
                        'geographical_coverage': geo_coverage,
                        'identifier': identifier and identifier[0] or "",
                        'keywords': keyword_dict(keywords),
                        'rdf': strip_special_chars(dataset_rdf),
                    })
        
        return dataset
        
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
        

    def package_create(self, data_json):
        """ Create a package
        """
        resp = self.package_search(
            prop='identifier', value=data_json['identifier']
        )
        
        if resp is []:
            resp = self.__conn.action.package_create(data_json)
            logger.info('Package \'%s\' added.' % resp[u'name'])
        return resp

    def package_update(self, data_package):
        """ Update an existing package
        """
        
        resp = self.package_search(
            prop='title', value=data_package['title']
        )
        
        if resp:
            package = resp[0]
            package.update(data_package)
            resp = self.__conn.call_action(
                'package_update', data_dict=package, apikey=self.__apikey)
            logger.info('Package \'%s\' updated.' % resp[u'name'])
        
        return resp
    
    def package_delete(self, package_id):
        """ Delete a package by id
            return True if the operation success
                   False otherwise
        """
        try:
            resp = self.__conn.action.package_delete(id=package_id)
        except ckanapi.NotFound:
            logger.error('Package \'%s\' not found.' % package_id)
            return False
        except Exception, error:
            logger.error('Got an error to execute the command for %s.' % (
                package_id
            ))
            return False
        else:
            logger.info('Package \'%s\' deleted.' % package_name)
        
        return True
        
        
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

if __name__ == '__main__':
    
    #query dataset
    odp = ODPClient()
    package = odp.package_show(u'XsJfLAZ4guXeAL4bjHNA')
    print package
    package = odp.package_search(prop='name', value=u'FPi519FhZ8UHVCNmdjhqPg')
    package = odp.package_search(prop='identifier', value=u'biogeographical-regions-europes')
    print package
