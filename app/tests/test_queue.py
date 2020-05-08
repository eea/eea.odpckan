import ckanclient


def test_queue_message_handler(mocker):
    cc = ckanclient.CKANClient('odp_queue')
    publish_dataset = mocker.patch.object(cc, 'publish_dataset')
    url = "http://www.eea.europa.eu/data-and-maps/data/european-union-emissions-trading-scheme-12"

    success_1 = cc.message_callback("delete|" + url + "|_ignored")
    assert success_1
    assert not publish_dataset.called

    success_2 = cc.message_callback("create|" + url + "|_ignored")
    assert success_2
    publish_dataset.assert_called_once_with(url)

    mocker.resetall()
    assert cc.message_callback("update|" + url + "|_ignored")
    publish_dataset.assert_called_once_with(url)

    success_3 = cc.message_callback("invalid_message")
    assert not success_3
