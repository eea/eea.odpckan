from pathlib import Path
from contextlib import contextmanager
import os

from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import DCTERMS, XSD, FOAF, RDF

import odpclient
import ckanclient
import sdsclient

DCAT = Namespace(u'http://www.w3.org/ns/dcat#')
VCARD = Namespace(u'http://www.w3.org/2006/vcard/ns#')
ADMS = Namespace(u'http://www.w3.org/ns/adms#')
SCHEMA = Namespace(u'http://schema.org/')
EU_FILE_TYPE = Namespace(u'http://publications.europa.eu/resource/authority/file-type/')
EU_DISTRIBUTION_TYPE = Namespace(u'http://publications.europa.eu/resource/authority/distribution-type/')
EU_LICENSE = Namespace(u'http://publications.europa.eu/resource/authority/licence/')
EU_STATUS = Namespace(u'http://publications.europa.eu/resource/authority/dataset-status/')
EU_COUNTRY = Namespace(u'http://publications.europa.eu/resource/authority/country/')

sds_responses = Path(__file__).resolve().parent / 'sds_responses'

SDS_MOCK_SPY = os.environ.get('SDS_MOCK_SPY')


@contextmanager
def mock_sds(mocker, filename):
    rdf_path = sds_responses / filename

    if SDS_MOCK_SPY:
        query_sds = mocker.spy(sdsclient.SDSClient, 'query_sds')
    else:
        query_sds = mocker.patch.object(sdsclient.SDSClient, 'query_sds')
        with rdf_path.open('rb') as f:
            query_sds.return_value = (f.read(), "")

    yield

    if SDS_MOCK_SPY:
        rdf = query_sds.spy_return[0]
        with rdf_path.open('wb') as f:
            f.write(rdf)


def test_query_sds_and_render_rdf(mocker):
    product_id = 'DAT-21-en'
    dataset_url = 'http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12'

    ok_tags = ["industry", "emission trading", "co2", "eu ets"]

    def tag_search(name):
        if name in ok_tags:
            return {'count': 1, 'results': [{'name': name}]}
        else:
            return {'count': 0}

    mocker.patch.object(odpclient.ODPClient, 'tag_search').side_effect = tag_search

    cc = ckanclient.CKANClient('odp_queue')

    with mock_sds(mocker, product_id + '.rdf'):
        dataset_rdf, dataset_json, msg = cc.get_dataset_data(dataset_url, product_id)
        assert not msg

    ckan_uri = cc.odp.get_ckan_uri(product_id)
    ckan_rdf = cc.odp.render_ckan_rdf(ckan_uri, product_id, dataset_rdf)

    g = Graph().parse(data=ckan_rdf)

    dataset = URIRef("http://data.europa.eu/88u/dataset/DAT-21-en")
    assert g.value(dataset, DCTERMS.identifier) == Literal(u'DAT-21-en')

    assert g.value(dataset, DCTERMS.issued) == Literal("2019-07-05T07:01:13+00:00", datatype=XSD.dateTime)
    assert g.value(dataset, DCTERMS.modified) == Literal("2019-10-30T10:59:23+00:00", datatype=XSD.dateTime)
    assert g.value(dataset, DCTERMS.publisher) == \
        URIRef("http://publications.europa.eu/resource/authority/corporate-body/EEA")
    assert g.value(dataset, DCTERMS.title) == \
        Literal("European Union Emissions Trading System (EU ETS) data from EUTL", lang="en")
    assert g.value(dataset, DCTERMS.description) == \
        Literal("Data about the EU emission trading system (ETS). The EU ETS data viewer provides "
                "aggregated data on emissions and allowances, by country, sector and year. The data "
                "mainly comes from the EU Transaction Log (EUTL). Additional information on "
                "auctioning and scope corrections is included.", lang="en")
    assert g.value(dataset, DCAT.theme) == \
        URIRef("http://publications.europa.eu/resource/authority/data-theme/ENVI")

    assert set(g.objects(dataset, DCAT.keyword)) == {Literal(k) for k in ok_tags}

    spatial = set(g.objects(dataset, DCTERMS.spatial))
    assert len(spatial) == 30
    assert EU_COUNTRY.DNK in spatial
    assert EU_COUNTRY.PRT in spatial

    contact = g.value(dataset, DCAT.contactPoint)
    assert g.value(contact, VCARD['organisation-name']) == Literal("European Environment Agency")
    homepage = g.value(contact, FOAF.homePage)
    assert g.value(homepage, SCHEMA.url) == Literal("https://www.eea.europa.eu")
    assert g.value(homepage, DCTERMS.title) == Literal("European Environment Agency", lang="en")
    telephone = g.value(contact, VCARD.hasTelephone)
    assert g.value(telephone, VCARD.hasValue) == URIRef("tel:+4533367100")
    address = g.value(contact, VCARD.hasAddress)
    assert g.value(address, VCARD['street-address']) == Literal("Kongens Nytorv 6, 1050 Copenhagen K, Denmark")

    landingpage = g.value(dataset, DCAT.landingPage)
    assert g.value(landingpage, SCHEMA.url) == \
        Literal("https://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12")
    assert g.value(landingpage, RDF.type) == FOAF.Document
    assert g.value(landingpage, FOAF.topic) == dataset
    assert g.value(landingpage, DCTERMS.title) == \
        Literal("European Union Emissions Trading System (EU ETS) data from EUTL", lang="en")

    dist = {}
    for d in g.objects(dataset, DCAT.distribution):
        url = g.value(d, DCAT.accessURL).toPython()
        assert url
        dist[url] = d

    assert len(dist) == 25
    assert len([d for d in dist if "OLDER VERSION" in str(g.value(dist[d], DCTERMS.title))]) == 19

    v8_url = "http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-8"
    dist_v8 = dist[v8_url]
    assert g.value(dist_v8, DCTERMS.title) == Literal("OLDER VERSION", lang="en")
    assert g.value(dist_v8, DCTERMS['format']) == EU_FILE_TYPE.HTML
    assert g.value(dist_v8, DCTERMS.type) == EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE
    assert g.value(dist_v8, DCAT.accessURL) == URIRef(v8_url)

    pdf_url = ("http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12/"
               "technical-report/technical-document/view")
    dist_pdf = dist[pdf_url]
    assert g.value(dist_pdf, DCTERMS.title) == Literal("Technical document", lang="en")
    assert g.value(dist_pdf, DCTERMS['format']) == EU_FILE_TYPE.PDF
    assert g.value(dist_pdf, DCTERMS.type) == EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE
    assert g.value(dist_pdf, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_pdf, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_pdf, DCAT.accessURL) == URIRef(pdf_url)

    xlsx_url = ("http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12/"
                "eu-ets-background-note/translation-of-activity-codes/view")
    dist_xlsx = dist[xlsx_url]
    assert g.value(dist_xlsx, DCTERMS.title) == Literal("Translation of activity codes", lang="en")
    assert g.value(dist_xlsx, DCTERMS['format']) == EU_FILE_TYPE.XLSX
    assert g.value(dist_xlsx, DCTERMS.type) == EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE
    assert g.value(dist_xlsx, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_xlsx, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_xlsx, DCAT.accessURL) == URIRef(xlsx_url)

    zip_url = ("http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12/"
               "eu-ets-data-download-latest-version/citl_v20.zip/view")
    dist_zip = dist[zip_url]
    assert g.value(dist_zip, DCTERMS.title) == Literal("ETS_Database_v34.zip", lang="en")
    assert g.value(dist_zip, DCTERMS['format']) \
        in [EU_FILE_TYPE.ZIP, EU_FILE_TYPE.CSV]  # TODO both values in sds response
    assert g.value(dist_zip, DCTERMS.type) == EU_DISTRIBUTION_TYPE.DOWNLOADABLE_FILE
    assert g.value(dist_zip, DCTERMS.license) == EU_LICENSE.CC_BY_4_0
    assert g.value(dist_zip, ADMS.status) == EU_STATUS.COMPLETED
    assert g.value(dist_zip, DCAT.accessURL) == URIRef(zip_url)

    # TODO test for distribution of type visualization
