from pathlib import Path

from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import DCTERMS, XSD, FOAF, RDF

import odpclient
import ckanclient
import sdsclient

DCAT = Namespace(u'http://www.w3.org/ns/dcat#')
VCARD = Namespace(u'http://www.w3.org/2006/vcard/ns#')
SCHEMA = Namespace(u'http://schema.org/')

sds_responses = Path(__file__).resolve().parent / 'sds_responses'


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

    # query_sds = mocker.spy(sdsclient.SDSClient, 'query_sds')
    query_sds = mocker.patch.object(sdsclient.SDSClient, 'query_sds')
    with (sds_responses / (product_id + '.rdf')).open() as f:
        query_sds.return_value = (f.read(), "")

    cc = ckanclient.CKANClient('odp_queue')

    dataset_rdf, dataset_json, msg = cc.get_dataset_data(dataset_url, product_id)
    assert not msg

    ckan_uri = cc.odp.get_ckan_uri(product_id)
    ckan_rdf = cc.odp.render_ckan_rdf(ckan_uri, dataset_json)

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

    contact = g.value(dataset, DCAT.contactPoint)
    assert g.value(contact, VCARD['organisation-name']) == Literal("European Environment Agency")
    address = g.value(contact, VCARD.hasAddress)
    assert g.value(address, VCARD['street-address']) == Literal("Kongens Nytorv 6, 1050 Copenhagen K, Denmark")

    landingpage = g.value(dataset, DCAT.landingPage)
    assert g.value(landingpage, SCHEMA.url) == \
        Literal("https://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12")
    assert g.value(landingpage, RDF.type) == FOAF.Document
    assert g.value(landingpage, FOAF.topic) == dataset
    assert g.value(landingpage, DCTERMS.title) == \
        Literal("European Union Emissions Trading System (EU ETS) data from EUTL", lang="en")
