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

#preload some defaults to handle missing sections/options
try:
    SECRET_RABBITMQ_HOST = parser.get('RABBITMQ', 'HOST')
except:
    SECRET_RABBITMQ_HOST = 'missing'
try:
    SECRET_RABBITMQ_PORT = parser.get('RABBITMQ', 'PORT')
except:
    SECRET_RABBITMQ_PORT = '5672'
try:
    SECRET_CKAN_ADDRESS = parser.get('CKAN', 'ADDRESS')
except:
    SECRET_CKAN_ADDRESS = 'missing'

rabbit_config = {
    'rabbit_host': os.environ.get('RABBITMQ_HOST', SECRET_RABBITMQ_HOST),
    'rabbit_port': int(os.environ.get('RABBITMQ_PORT', SECRET_RABBITMQ_PORT)),
    'rabbit_username': parser.get('RABBITMQ', 'USERNAME'),
    'rabbit_password': parser.get('RABBITMQ', 'PASSWORD')
}

ckan_config = {
    'ckan_address': os.environ.get('CKAN_ADDRESS', SECRET_CKAN_ADDRESS),
    'ckan_apikey': parser.get('CKAN', 'APIKEY')
}

services_config = {
    'eea': os.environ.get('SERVICES_EEA', parser.get('SERVICES', 'EEA')),
    'sds': os.environ.get('SERVICES_SDS', parser.get('SERVICES', 'SDS')),
    'odp': os.environ.get('SERVICES_ODP', parser.get('SERVICES', 'ODP'))
}

other_config = {
    'timeout': 60   #timeout used for opening URLs
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
