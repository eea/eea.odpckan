""" SDS client - methods to query/get a dataset data from
    SDS (http://semantic.eea.europa.eu/)
"""

import argparse
import json
import re

import requests
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import DCTERMS, RDF
from eea.rabbitmq.client import RabbitMQConnector

from config import logger, services_config, rabbit_config, other_config
from odpclient import ODPClient

DCAT = Namespace("http://www.w3.org/ns/dcat#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
ADMS = Namespace("http://www.w3.org/ns/adms#")
SCHEMA = Namespace("http://schema.org/")
EU_FILE_TYPE = Namespace(
    "http://publications.europa.eu/resource/authority/file-type/"
)
EU_DISTRIBUTION_TYPE = Namespace(
    "http://publications.europa.eu/resource/authority/distribution-type/"
)
EU_LICENSE = Namespace(
    "http://publications.europa.eu/resource/authority/licence/"
)
EU_STATUS = Namespace(
    "http://publications.europa.eu/resource/authority/dataset-status/"
)
EU_COUNTRY = Namespace(
    "http://publications.europa.eu/resource/authority/country/"
)
EUROVOC = Namespace("http://eurovoc.europa.eu/")
ECODP = Namespace("http://open-data.europa.eu/ontologies/ec-odp#")
DAVIZ = Namespace("http://www.eea.europa.eu/portal_types/DavizVisualization#")
GIS = Namespace("http://www.eea.europa.eu/portal_types/GIS%%20Application#")
EEAFIGURE = Namespace("http://www.eea.europa.eu/portal_types/EEAFigure#")
DASHBOARD = Namespace("http://www.eea.europa.eu/portal_types/Dashboard#")
INFOGRAPHIC = Namespace("http://www.eea.europa.eu/portal_types/Infographic#")


FILE_TYPES = {
    "application/msaccess": "MDB",
    "application/msword": "DOC",
    "application/octet-stream": "OCTET",
    "application/pdf": "PDF",
    "application/vnd.google-earth.kml+xml": "KML",
    "application/vnd.ms-excel": "XLS",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        "XLSX",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        "DOCX",
    "application/x-dbase": "DBF",
    "application/x-e00": "E00",
    "application/xml": "XML",
    "application/zip": "ZIP",
    "image/gif": "GIF",
    "image/jpeg": "JPEG",
    "image/png": "PNG",
    "image/tiff": "TIFF",
    "text/comma-separated-values": "CSV",
    "text/csv": "CSV",
    "text/html": "HTML",
    "text/plain": "TXT",
    "text/xml": "XML",
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

    def parse_datasets_json(self, datasets_json):
        """ Parses a response with datasets from SDS in JSON format.
        """
        r = []
        for item_json in datasets_json["results"]["bindings"]:
            product_id = item_json["product_id"]["value"]
            dataset_url = item_json["dataset"]["value"]
            r.append((dataset_url, product_id))
        return r

    def query_sds(self, query, format):
        """ Generic method to query SDS to be used all around.
        """
        data = {"query": query, "format": format}
        headers = {"Accept": format}
        resp = requests.post(self.endpoint, data=data, headers=headers)
        return resp.text

    def query_dataset(self, dataset_url):
        """ Given a dataset URL interogates the SDS service
            about it and returns the result which is RDF.
        """
        logger.info("query dataset '%s'", dataset_url)
        query = other_config["query_dataset"] % {"dataset": dataset_url}
        return self.query_sds(query, "application/xml")

    def get_latest_version(self, dataset_url):
        """ Given a dataset URL interogates the SDS service
            and returns the latest version URI.
        """
        logger.info("query latest version '%s'", dataset_url)
        query = other_config["query_latest_version"] % {"dataset": dataset_url}
        resp = self.query_sds(query, "application/json")
        bindings = json.loads(resp)["results"]["bindings"]
        if bindings:
            return bindings[0]["latest"]["value"]
        else:
            return dataset_url

    def query_all_datasets(self):
        """ Find all datasets (to pe updated in ODP) in the repository.
        """
        logger.info("query all datasets")
        query = other_config["query_all_datasets"]
        result = self.query_sds(query, "application/json")
        return json.loads(result)

    def query_replaces(self):
        """ Find which datasets replace other datasets
        """
        logger.info("query replaces")
        query = other_config["query_replaces"]
        result = self.query_sds(query, "application/json")
        return json.loads(result)

    def get_rabbit(self):
        rabbit = RabbitMQConnector(**rabbit_config)
        rabbit.open_connection()
        rabbit.declare_queue(self.queue_name)
        return rabbit

    def add_to_queue(
        self, rabbit, action, dataset_url, dataset_identifier, counter=1
    ):
        body = "%(action)s|%(dataset_url)s|%(dataset_identifier)s" % {
            "action": action,
            "dataset_url": dataset_url,
            "dataset_identifier": dataset_identifier,
        }
        logger.info(
            "BULK update %s: sending '%s' in '%s'",
            counter,
            body,
            self.queue_name,
        )
        rabbit.send_message(self.queue_name, body)

    def bulk_update(self):
        """ Queries SDS for all datasets and injects messages in rabbitmq.
        """
        logger.info("START bulk update")
        result_json = self.query_all_datasets()
        datasets_json = result_json["results"]["bindings"]
        logger.info("BULK update: %s datasets found", len(datasets_json))
        rabbit = self.get_rabbit()
        counter = 1
        for item_json in datasets_json:
            dataset_url = item_json["dataset"]["value"]
            action = "update"
            self.add_to_queue(
                rabbit,
                action,
                dataset_url,
                "_fake_dataset_identifier_",
                counter,
            )
            counter += 1
        rabbit.close_connection()
        logger.info("DONE bulk update")

    def parse_dataset(self, dataset_rdf, dataset_url, check_obsolete=True):
        """
            refs: http://dataprotocols.org/data-packages/
        """
        g = Graph().parse(data=dataset_rdf)
        dataset = URIRef(dataset_url)

        if check_obsolete and g.value(dataset, DCTERMS.isReplacedBy):
            raise RuntimeError("Dataset %r is obsolete" % dataset_url)

        def https_link(url):
            return re.sub(r"^http://", "https://", url)

        def convert_directlink_to_view(url):
            """ replace direct links to files to the corresponding web page
            """
            return https_link(url.replace("/at_download/file", "/view"))

        def file_type(mime_type):
            name = FILE_TYPES.get(mime_type, "OCTET")
            return EU_FILE_TYPE[name]

        keywords = [str(k) for k in g.objects(dataset, ECODP.keyword)]
        geo_coverage = [str(k) for k in g.objects(dataset, DCTERMS.spatial)]
        concepts_eurovoc = [
            str(k)
            for k in g.objects(dataset, DCAT.theme)
            if str(k).startswith(str(EUROVOC))
        ]

        resources = []

        visualisation_types = [DAVIZ.DavizVisualization, GIS.GisApplication, EEAFIGURE.EEAFigure,
                               INFOGRAPHIC.Infographic, DASHBOARD.Dashboard]
        for res in g.objects(dataset, DCAT.distribution):
            types = list(g.objects(res, RDF.type))

            if any([True if visual_type in types else False for visual_type in visualisation_types]):
                distribution_type = EU_DISTRIBUTION_TYPE.VISUALIZATION

            elif URIRef("http://www.w3.org/TR/vocab-dcat#Download") in types:
                distribution_type = EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE

            else:
                raise RuntimeError("Unknown distribution type %r", res)

            file_types = {
                str(v) for v in g.objects(res, ECODP.distributionFormat)
            }
            if len(file_types) > 1:
                if "application/zip" in file_types:
                    file_types.remove("application/zip")
                if len(file_types) > 1:
                    logger.warning(
                        "Found multiple distribution formats: %r", file_types
                    )

            resources.append(
                {
                    "title": str(g.value(res, DCTERMS.title)),
                    "filetype": file_type(list(file_types)[0]),
                    "url": convert_directlink_to_view(
                        str(g.value(res, DCAT.accessURL))
                    ),
                    "distribution_type": distribution_type,
                    "status": EU_STATUS.COMPLETED,
                }
            )

        for old in g.objects(dataset, DCTERMS.replaces):
            issued = g.value(old, DCTERMS.issued).toPython().date()
            resources.append(
                {
                    "title": "OLDER VERSION - %s" % issued,
                    "description": str(g.value(old, DCTERMS.description)),
                    "filetype": file_type("text/html"),
                    "url": https_link(str(old)),
                    "distribution_type":
                        EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE,
                    "status": EU_STATUS.DEPRECATED,
                }
            )
        product_id = str(g.value(dataset, SCHEMA.productID))
        landing_page = "https://www.eea.europa.eu/ds_resolveuid/" + product_id

        return {
            "product_id": product_id,
            "title": str(g.value(dataset, DCTERMS.title)),
            "description": str(g.value(dataset, DCTERMS.description)),
            "landing_page": landing_page,
            "issued": str(g.value(dataset, DCTERMS.issued)),
            "metadata_modified": str(g.value(dataset, DCTERMS.modified)),
            "status": str(EU_STATUS.COMPLETED),
            "keywords": keywords,
            "geographical_coverage": geo_coverage,
            "concepts_eurovoc": concepts_eurovoc,
            "resources": resources,
        }

    def get_dataset(self, dataset_url, check_obsolete=True):
        dataset_rdf = self.query_dataset(dataset_url)
        return self.parse_dataset(dataset_rdf, dataset_url, check_obsolete)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SDSClient")
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="creates debug files for datasets queries",
    )
    args = parser.parse_args()

    sds = SDSClient(
        services_config["sds"],
        other_config["timeout"],
        "odp_queue",
        ODPClient(),
    )

    if args.debug:
        dataset_url = (
            "http://www.eea.europa.eu/data-and-maps/data/"
            "european-union-emissions-trading-scheme-13"
        )
        _rabbit = sds.get_rabbit()
        sds.add_to_queue(
            _rabbit, "update", dataset_url, "_fake_dataset_identifier_"
        )
        sds.add_to_queue(
            _rabbit, "delete", dataset_url, "_fake_dataset_identifier_"
        )
        _rabbit.close_connection()

    else:
        # initiate a bulk update operation
        sds.bulk_update()
