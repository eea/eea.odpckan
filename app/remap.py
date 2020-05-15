"""
Identify old datasets, mark them as obsolete, and generate a mapping to newly
published datasets that are identified by ProductID.

Usage:

1. Set the ``OLD_DATASETS_REPO`` environment variable to a directory for
temporary files::

    mkdir /tmp/old-datasets
    export OLD_DATASETS_REPO=/tmp/old-datasets

1. Download current datasets and match them::

    python remap.py download
    python remap.py match_datasets

2. Generate a CSV with a mapping between old and new::

    python remap.py old_new_mapping > /tmp/dataset_mapping.csv

3. Re-publish the old datasets as "obsolete"::

    python remap.py mark_obsolete
"""

import sys
import re
import json
import argparse
from pathlib import Path
import csv

import requests

from config import logger, other_config
from ckanclient import CKANClient
from sdsclient import EU_STATUS


class RemapDatasets:

    odp_uri_prefix = "http://data.europa.eu/88u/dataset/"

    def __init__(self, repo):
        self.repo = repo
        self.datasets_csv = self.repo / "datasets.csv"

        self.cc = CKANClient("odp_queue")
        self.odp = self.cc.odp
        self.sds = self.cc.sds

        self.product_id_map = {}
        for res in self.sds.query_replaces()["results"]["bindings"]:
            self.product_id_map[res["dataset"]["value"]] = res["product_id"][
                "value"
            ]

    def download(self):
        for n, item in enumerate(
            self.odp.package_search(fq="organization:eea")
        ):
            uri = item["dataset"]["uri"]
            assert uri.startswith(self.odp_uri_prefix)
            id = uri[len(self.odp_uri_prefix):]
            print(n, id)
            with (self.repo / (id + ".json")).open("w", encoding="utf8") as f:
                f.write(json.dumps(item, indent=2, sort_keys=True))

    def resolve_url(self, url):
        while True:
            resp = requests.head(url)
            if not resp.is_redirect:
                return url
            url = resp.next.url

    def iter_datasets(self):
        for p in self.repo.iterdir():
            if not p.name.endswith(".json"):
                continue
            with p.open(encoding="utf8") as f:
                item = json.load(f)
            yield item

    def match_datasets(self):
        with self.datasets_csv.open("w", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ckan_uri", "product_id", "url"])

            for item in self.iter_datasets():
                uri = item["dataset"]["uri"]

                try:
                    _row = item["dataset"]["landingPage_dcat"][0]
                    landing_page = _row["url_schema"][0]["value_or_uri"]
                except KeyError:
                    logger.warning("No landing page for dataset: %r", uri)
                    continue

                url = landing_page
                url = re.sub(r"^https://", "http://", url)

                if url in self.product_id_map:
                    writer.writerow([uri, self.product_id_map[url], url])
                    continue

                if "www.eea.europa.eu/data-and-maps" not in url:
                    logger.warning("Not a dataset: %r, landing page: %r",
                                   uri, landing_page)
                    continue

                url = self.resolve_url(url)
                url = re.sub(r"^https://", "http://", url)
                if url in self.product_id_map:
                    writer.writerow([uri, self.product_id_map[url], url])
                    continue

                logger.warning(
                    "Could not find product_id for dataset: "
                    "%r, landing page: %r",
                    uri,
                    landing_page,
                )

    def old_new_mapping(self):
        current = set()
        mapping = {}
        with self.datasets_csv.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uri = row["ckan_uri"]
                product_id = row["product_id"]
                current_uri = self.odp_uri_prefix + product_id

                if uri == current_uri:
                    current.add(uri)

                else:
                    mapping[uri] = current_uri

            for uri in set(mapping.values()) - current:
                logger.warning("Dataset is not published: %r", uri)

            writer = csv.writer(sys.stdout)
            writer.writerow(["source", "destination"])
            for s, d in mapping.items():
                writer.writerow([s, d])

    def mark_obsolete(self):
        product_ids = set(self.product_id_map.values())
        with self.datasets_csv.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ckan_uri = row["ckan_uri"]
                url = row["url"]

                if not url:
                    continue

                identifier = ckan_uri.split("/")[-1]

                if identifier in product_ids:
                    logger.warning("Dataset %r is current, skipping", ckan_uri)
                    continue

                self.publish_dataset(url, ckan_uri)

    def publish_dataset(self, dataset_url, ckan_uri):
        """ Publish dataset to ODP
        """
        logger.info("publish obsolete dataset '%s'", dataset_url)

        if dataset_url.startswith("https"):
            dataset_url = dataset_url.replace("https", "http", 1)

        data = self.sds.get_dataset(dataset_url, check_obsolete=False)
        data["uri"] = ckan_uri

        data["status"] = str(EU_STATUS.DEPRECATED)
        data["resources"][100:] = []
        for r in data["resources"]:
            r["status"] = str(EU_STATUS.DEPRECATED)
        data["title"] = "[DEPRECATED] " + data["title"]

        if data["issued"] == "None":
            data["issued"] = data["metadata_modified"]

        ckan_rdf = self.cc.render_ckan_rdf(data)
        with open("/tmp/publish.rdf", "w", encoding="utf-8") as f:
            f.write(ckan_rdf)
        self.odp.package_save(ckan_uri, ckan_rdf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remap datasets")
    parser.add_argument("action")

    args = parser.parse_args()

    assert other_config["old_datasets_repo"]
    repo = Path(other_config["old_datasets_repo"])
    rd = RemapDatasets(repo)

    if args.action == "download":
        rd.download()

    elif args.action == "match_datasets":
        rd.match_datasets()

    elif args.action == "old_new_mapping":
        rd.old_new_mapping()

    elif args.action == "mark_obsolete":
        rd.mark_obsolete()

    else:
        raise RuntimeError("Unknown action %r" % args.action)
