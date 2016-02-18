""" ODP client - use data from SDS in JSON format and update the ODP
    SDS (http://semantic.eea.europa.eu/)
    ODP (https://open-data.europa.eu/en/data/publisher/eea)

"""

import ckanapi
import pprint

from config import logger, ckan_config, services_config

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

    def package_create(self, organization_id, package_name, title,
                       description=u'', author=None, author_email=None,
                       maintainer=None, maintainer_email=None,
                       license_id=u'cc-by', url=None, version=None,
                       state=u'active', type=u'dataset'):
        """ Create a package
        """
        resp = self.package_show(package_name)
        if resp is None:
            resp = self.__conn.action.package_create(owner_org=organization_id,
                name=package_name, title=title,
                notes=description, author=author,
                author_email=author_email, maintainer=maintainer,
                maintainer_email=maintainer_email, license_id=license_id,
                url=url, version=version, state=state, type=type)
            logger.info('Package \'%s\' added.' % package_name)
        return resp

    def package_update(self, package):
        """ Update an existing package
        """
        resp = self.__conn.call_action('package_update', package)
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
    package = odp.package_show(u'FPi519FhZ8UHVCNmdjhqPg')
    package = odp.package_search(prop='name', value=u'FPi519FhZ8UHVCNmdjhqPg')
    package = odp.package_search(prop='identifier', value=u'corilis-2000-2')