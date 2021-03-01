"""
    This test suite checks that all the requested visualisation types are obtained in the sparql queries.
    Each test checks for either a forward or backward visualisation related item.
"""

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS

import ckanclient
from sdsclient import (
    DCAT,
    ADMS,
    EU_FILE_TYPE,
    EU_DISTRIBUTION_TYPE,
    EU_LICENSE,
    EU_STATUS,
)

from .conftest import mock_sds


def dataset_to_graph(mocker, product_id, dataset_url):
    cc = ckanclient.CKANClient("odp_queue")

    mocker.patch.object(cc.odp, "package_show").return_value = None
    mocker.patch.object(cc.sds, "get_latest_version").side_effect = lambda d: d

    package_save = mocker.patch.object(cc.odp, "package_save")
    with mock_sds(mocker, product_id + ".rdf"):
        cc.publish_dataset(dataset_url)

    ckan_rdf = package_save.call_args[0][1]

    g = Graph().parse(data=ckan_rdf)
    return g


def test_query_retrieve_forward_eea_figures(mocker):
    product_id = "DAT-137-en"
    dataset_url = (
        "http://www.eea.europa.eu/data-and-maps/data/"
        "eunis-habitat-classification"
    )
    g = dataset_to_graph(mocker, product_id, dataset_url)

    dataset = URIRef("https://www.eea.europa.eu/ds_resolveuid/" + product_id)

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    eeafigure_url = (
        "https://www.eea.europa.eu/data-and-maps/figures/"
        "eunis-habitat-types-per-biogeographic-region"
    )
    dist_eeafigure = dist[eeafigure_url]
    assert g.value(dist_eeafigure, DCTERMS.title) == Literal(
        "EUNIS habitat types per biogeographic region", lang="en"
    )
    assert g.value(dist_eeafigure, DCTERMS["format"]) == EU_FILE_TYPE.HTML
    assert (
            g.value(dist_eeafigure, DCTERMS.type) == EU_DISTRIBUTION_TYPE.VISUALIZATION
    )
    assert g.value(dist_eeafigure, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_eeafigure, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_eeafigure, DCAT.accessURL) == URIRef(eeafigure_url)


def test_query_retrieve_forward_daviz(mocker):
    product_id = "DAT-21-en"
    dataset_url = (
        "http://www.eea.europa.eu/data-and-maps/data/"
        "european-union-emissions-trading-scheme-13"
    )
    g = dataset_to_graph(mocker, product_id, dataset_url)

    dataset = URIRef("https://www.eea.europa.eu/ds_resolveuid/" + product_id)

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    daviz_url = (
        "https://www.eea.europa.eu/data-and-maps/daviz/"
        "eu-ets-emissions-by-activity-type"
    )
    dist_daviz = dist[daviz_url]
    assert g.value(dist_daviz, DCTERMS.title) == Literal(
        "EU ETS emissions by activity type", lang="en"
    )
    assert g.value(dist_daviz, DCTERMS["format"]) == EU_FILE_TYPE.HTML
    assert (
            g.value(dist_daviz, DCTERMS.type) == EU_DISTRIBUTION_TYPE.VISUALIZATION
    )
    assert g.value(dist_daviz, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_daviz, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_daviz, DCAT.accessURL) == URIRef(daviz_url)


def test_query_retrieve_forward_gis_application(mocker):
    """
        TODO: At the moment there are only two GIS Applications visualisations, both of
        them being expired. The SPARQL queries check for visualisations with no
        expiration dates. When a relevant example will exist, a test should be implemented.

        The query that displays all the GIS Application visualisations alongside their expirations is:
        PREFIX a: <http://www.eea.europa.eu/portal_types/Data#>
        PREFIX gis: <http://www.eea.europa.eu/portal_types/GIS%20Application#>
        PREFIX dct: <http://purl.org/dc/terms/>

        SELECT ?dataset ?gis ?related_item_expires
        WHERE {
            ?dataset a a:Data ;
                a:relatedItems ?gis.
            ?gis a gis:GISApplication;
                   dct:expires ?related_item_expires .
        }
        LIMIT 50
    """


def test_query_retrieve_backward_infographic(mocker):
    product_id = "DAT-150-en"
    dataset_url = (
        "http://www.eea.europa.eu/data-and-maps/data/"
        "air-pollutant-concentrations-at-station"
    )
    g = dataset_to_graph(mocker, product_id, dataset_url)

    dataset = URIRef("https://www.eea.europa.eu/ds_resolveuid/" + product_id)

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    backward_infographic_url = (
        "https://www.eea.europa.eu/media/infographics/"
        "many-europeans-are-exposed-to-2"
    )
    dist_infograhic = dist[backward_infographic_url]
    assert g.value(dist_infograhic, DCTERMS.title) == Literal(
        "Many Europeans are exposed to harmful levels of air pollution", lang="en"
    )
    assert g.value(dist_infograhic, DCTERMS["format"]) == EU_FILE_TYPE.HTML
    assert (
            g.value(dist_infograhic, DCTERMS.type) == EU_DISTRIBUTION_TYPE.VISUALIZATION
    )
    assert g.value(dist_infograhic, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_infograhic, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_infograhic, DCAT.accessURL) == URIRef(backward_infographic_url)


def test_query_retrieve_backward_eea_figure(mocker):
    product_id = "DAT-150-en"
    dataset_url = (
        "http://www.eea.europa.eu/data-and-maps/data/"
        "air-pollutant-concentrations-at-station"
    )
    g = dataset_to_graph(mocker, product_id, dataset_url)

    dataset = URIRef("https://www.eea.europa.eu/ds_resolveuid/" + product_id)

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    backward_eea_figure_url = (
        "https://www.eea.europa.eu/data-and-maps/figures/"
        "urban-population-resident-in-areas-pollutant-limit-target"
    )
    dist_eea_figure = dist[backward_eea_figure_url]
    assert g.value(dist_eea_figure, DCTERMS.title) == Literal(
        "Percentage of urban population resident in areas where pollutant concentrations "
        "are higher than selected limit/target values, 2000-2012 (EU-28)", lang="en"
    )
    assert g.value(dist_eea_figure, DCTERMS["format"]) == EU_FILE_TYPE.HTML
    assert (
            g.value(dist_eea_figure, DCTERMS.type) == EU_DISTRIBUTION_TYPE.VISUALIZATION
    )
    assert g.value(dist_eea_figure, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_eea_figure, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_eea_figure, DCAT.accessURL) == URIRef(backward_eea_figure_url)


def test_query_retrieve_backward_dashboard(mocker):
    product_id = "DAT-176-en"
    dataset_url = (
        "http://www.eea.europa.eu/data-and-maps/data/"
        "fuel-quality-directive-1"
    )
    g = dataset_to_graph(mocker, product_id, dataset_url)

    dataset = URIRef("https://www.eea.europa.eu/ds_resolveuid/" + product_id)

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    backward_dashboard_url = (
        "https://www.eea.europa.eu/data-and-maps/dashboards/"
        "fuel-quality-article-8"
    )
    print(dist.keys())
    dist_dashboard = dist[backward_dashboard_url]
    assert g.value(dist_dashboard, DCTERMS.title) == Literal(
        "Petrol and diesel fuels sold for road transport", lang="en"
    )
    assert g.value(dist_dashboard, DCTERMS["format"]) == EU_FILE_TYPE.HTML
    assert (
            g.value(dist_dashboard, DCTERMS.type) == EU_DISTRIBUTION_TYPE.VISUALIZATION
    )
    assert g.value(dist_dashboard, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_dashboard, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_dashboard, DCAT.accessURL) == URIRef(backward_dashboard_url)
