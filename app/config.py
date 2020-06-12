""" Config - various parameters
"""
import os
import logging
import pprint

# setup logger
logger = logging.getLogger('eea.odpckan')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s/%(filename)s/%(funcName)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# setup configuration
rabbit_config = {
    'rabbit_host': os.environ.get('RABBITMQ_HOST'),
    'rabbit_port': os.environ.get('RABBITMQ_PORT'),
    'rabbit_username': os.environ.get('RABBITMQ_USERNAME'),
    'rabbit_password': os.environ.get('RABBITMQ_PASSWORD')
}

ckan_config = {
    'ckan_address': os.environ.get('CKAN_ADDRESS'),
    'ckan_apikey': os.environ.get('CKAN_APIKEY'),
    'ckan_proxy': os.environ.get('CKAN_PROXY'),
}

services_config = {
    'sds': os.environ.get('SERVICES_SDS'),
}


def load_sparql(fname):
    return open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', fname), 'r').read()


other_config = {
    'timeout': int(os.environ.get('SDS_TIMEOUT') or 60),
    'query_all_datasets': load_sparql('query_all_datasets.sparql'),
    'query_dataset': load_sparql('query_dataset.sparql'),
    'query_replaces': load_sparql('query_replaces.sparql'),
    'query_latest_version': load_sparql('query_latest_version.sparql'),
    'old_datasets_repo': os.environ.get('OLD_DATASETS_REPO'),
}


# debug tools
def dump_rdf(fname, value):
    """ Useful when debugging RDF results.
    """
    f = open(fname, 'wb')
    f.write(value.encode('utf8'))
    f.close()

def dump_json(fname, value):
    """ Useful when debugging JSON results.
    """
    f = open(fname, 'w')
    pprint.pprint(value, f)
    f.close()
