""" Config - various parameters
"""
import os
import logging
import pprint
from ConfigParser import SafeConfigParser

#setup logger
logger = logging.getLogger('eea.odpckan')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s/%(filename)s/%(funcName)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#read .secret files
parser = SafeConfigParser()
parser.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.secret'))

try:
    SECRET_RABBITMQ_HOST = parser.get('RABBITMQ', 'HOST')
except:
    SECRET_RABBITMQ_HOST = 'missing'
try:
    SECRET_RABBITMQ_PORT = parser.get('RABBITMQ', 'PORT')
except:
    SECRET_RABBITMQ_PORT = '5672'
rabbit_config = {
    'rabbit_host': os.environ.get('RABBITMQ_HOST', SECRET_RABBITMQ_HOST),
    'rabbit_port': int(os.environ.get('RABBITMQ_PORT', SECRET_RABBITMQ_PORT)),
    'rabbit_username': parser.get('RABBITMQ', 'USERNAME'),
    'rabbit_password': parser.get('RABBITMQ', 'PASSWORD')
}

try:
    SECRET_CKAN_ADDRESS = parser.get('CKAN', 'ADDRESS')
except:
    SECRET_CKAN_ADDRESS = 'missing'
ckan_config = {
    'ckan_address': os.environ.get('CKAN_ADDRESS', SECRET_CKAN_ADDRESS),
    'ckan_apikey': parser.get('CKAN', 'APIKEY')
}

try:
    SECRET_SERVICES_EEA = parser.get('SERVICES', 'EEA')
except:
    SECRET_SERVICES_EEA = 'missing'
try:
    SECRET_SERVICES_SDS = parser.get('SERVICES', 'SDS')
except:
    SECRET_SERVICES_SDS = 'missing'
try:
    SECRET_SERVICES_ODP = parser.get('SERVICES', 'ODP')
except:
    SECRET_SERVICES_ODP = 'missing'
services_config = {
    'eea': os.environ.get('SERVICES_EEA', SECRET_SERVICES_EEA),
    'sds': os.environ.get('SERVICES_SDS', SECRET_SERVICES_SDS),
    'odp': os.environ.get('SERVICES_ODP', SECRET_SERVICES_ODP)
}

def load_sparql(fname):
    return open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', fname), 'r').read()

other_config = {
    'timeout': 60,   #timeout used for opening URLs
    'query_all_datasets': load_sparql('query_all_datasets.sparql'),   #sparql query to get all datasets
    'query_dataset': load_sparql('query_dataset.sparql')   #sparql query to get a specified dataset
}

def dump_rdf(fname, value):
    """ Useful when debugging RDF results.
    """
    f = open(fname, 'w')
    f.write(value)
    f.close()

def dump_json(fname, value):
    """ Useful when debugging JSON results.
    """
    f = open(fname, 'w')
    pprint.pprint(value, f)
    f.close()
