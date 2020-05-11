import ckanclient

from conftest import mock_sds


def test_get_dataset_latest_version(mocker):
    product_id = 'DAT-21-en'
    dataset_url = (
        'http://www.eea.europa.eu/data-and-maps/data/'
        'european-union-emissions-trading-scheme-8'
    )

    cc = ckanclient.CKANClient('odp_queue')

    with mock_sds(mocker, product_id + '-latest.rdf'):
        latest_dataset_url = cc.sds.get_latest_version(dataset_url)

    assert latest_dataset_url == (
        'http://www.eea.europa.eu/data-and-maps/data/'
        'european-union-emissions-trading-scheme-12'
    )
