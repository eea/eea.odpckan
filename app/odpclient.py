""" ODP client - use data from SDS in JSON format and update the ODP
    SDS (http://semantic.eea.europa.eu/)
    ODP (https://open-data.europa.eu/en/data/publisher/eea)
"""

import ckanapi

from config import logger, ckan_config


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

        self.conn = ckanapi.RemoteCKAN(
            self.__address,
            self.__apikey,
            self.__user_agent,
            base_url='apiodp/action/',
            session=session,
        )
        logger.info('Connected to %s' % self.__address)

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
        return self.conn.call_action("package_save", data_dict=envelope)
