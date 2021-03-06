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
        self.__address = ckan_config["ckan_address"]
        self.__apikey = ckan_config["ckan_apikey"]
        self.__user_agent = user_agent

        if ckan_config["ckan_proxy"]:
            import requests

            session = requests.Session()
            session.proxies = {
                "http": ckan_config["ckan_proxy"],
                "https": ckan_config["ckan_proxy"],
            }
        else:
            session = None

        self.conn = ckanapi.RemoteCKAN(
            self.__address,
            self.__apikey,
            self.__user_agent,
            base_url="apiodp/action/",
            session=session,
        )
        logger.info("Connected to %s" % self.__address)

    def package_save(self, ckan_uri, ckan_rdf):
        """ Save a package
        """
        logger.info("Uploading dataset %r", ckan_uri)
        envelope = {
            "addReplaces": [
                {
                    "objectUri": ckan_uri,
                    "addReplace": {"objectStatus": "published"},
                }
            ],
            "rdfFile": ckan_rdf,
        }
        return self.conn.call_action("package_save", data_dict=envelope)

    def package_show(self, package_name):
        """ Get the package by name
        """
        try:
            return self.conn.action.package_show(id=package_name)
        except ckanapi.errors.NotFound:
            return None

    def package_search(self, fq):
        start = 0
        while True:
            resp = self.conn.action.package_search(
                fq=fq, output_format="json", start=start,
            )

            if not resp["results"]:
                return

            for item in resp["results"]:
                yield item
                start += 1
