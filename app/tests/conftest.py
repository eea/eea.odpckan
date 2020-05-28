from pathlib import Path
from contextlib import contextmanager
import os

import sdsclient

sds_responses = Path(__file__).resolve().parent / "sds_responses"

SDS_MOCK_SPY = os.environ.get("SDS_MOCK_SPY")


@contextmanager
def mock_sds(mocker, filename):
    """ Mock the SDS service.
        Returns pre-saved SDS responses from the "app/tests/sds_responses"
        directory. Run the tests with the "SDS_MOCK_SPY=true" environment
        variable to actually query SDS and save the responses.
    """
    rdf_path = sds_responses / filename

    if SDS_MOCK_SPY:
        query_sds = mocker.spy(sdsclient.SDSClient, "query_sds")
    else:
        query_sds = mocker.patch.object(sdsclient.SDSClient, "query_sds")
        with rdf_path.open("r", encoding="utf-8") as f:
            query_sds.return_value = f.read()

    yield

    if SDS_MOCK_SPY:
        rdf = query_sds.spy_return
        with rdf_path.open("w", encoding="utf-8") as f:
            f.write(rdf)
