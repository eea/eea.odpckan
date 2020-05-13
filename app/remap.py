"""
Identify old datasets, mark them as obsolete, and generate a mapping to newly
published datasets that are identified by ProductID.

Usage:

1. Set the ``OLD_DATASETS_REPO`` environment variable to a directory for
temporary files::

    mkdir /tmp/old-datasets
    export OLD_DATASETS_REPO=/tmp/old-datasets

1. Download current datasets::

    python remap.py download

2. Generate a CSV with a mapping between old and new::

    python remap.py old_new_mapping > /tmp/dataset_mapping.csv
"""

import sys
import re
import json
import argparse
from pathlib import Path
import csv

import requests

from config import logger, other_config, services_config
from odpclient import ODPClient
from sdsclient import SDSClient


class RemapDatasets:

    odp_uri_prefix = "http://data.europa.eu/88u/dataset/"

    def __init__(self, repo):
        self.repo = repo
        self.odp = ODPClient()
        self.sds = SDSClient(
            services_config["sds"],
            other_config["timeout"],
            "odp_queue",
            self.odp,
        )

    def download(self):
        for n, item in enumerate(
            self.odp.package_search(fq="organization:eea")
        ):
            uri = item["dataset"]["uri"]
            assert uri.startswith(self.odp_uri_prefix)
            id = uri[len(self.odp_uri_prefix) :]
            print(n, id)
            with (self.repo / f"{id}.json").open("w", encoding="utf8") as f:
                print(json.dumps(item, indent=2, sort_keys=True), file=f)

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

    def match_all(self):
        product_id_map = {}
        for res in self.sds.query_replaces()["results"]["bindings"]:
            product_id_map[res["dataset"]["value"]] = res["product_id"][
                "value"
            ]

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

            if url in product_id_map:
                yield uri, product_id_map[url]
                continue

            if "www.eea.europa.eu/data-and-maps" not in url:
                logger.warning(
                    "Not a dataset: %r, landing page: %r", uri, landing_page,
                )
                continue

            url = self.resolve_url(url)
            url = re.sub(r"^https://", "http://", url)
            if url in product_id_map:
                yield uri, product_id_map[url]
                continue

            logger.warning(
                "Could not find product_id for dataset: %r, landing page: %r",
                uri,
                landing_page,
            )

    def old_new_mapping(self):
        current = set()
        mapping = {}
        for uri, product_id in self.match_all():
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remap datasets")
    parser.add_argument("action")

    args = parser.parse_args()

    assert other_config["old_datasets_repo"]
    repo = Path(other_config["old_datasets_repo"])
    rd = RemapDatasets(repo)

    if args.action == "download":
        rd.download()

    elif args.action == "old_new_mapping":
        rd.old_new_mapping()

    else:
        raise RuntimeError(f"Unknown action {args.action!r}")
