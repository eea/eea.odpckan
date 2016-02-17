import logging
from ConfigParser import SafeConfigParser

#setup logger
logger = logging.getLogger('eea.odpckan')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s/%(filename)s/%(funcName)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#read secret
parser = SafeConfigParser()
parser.read('.secret')

rabbit_config = {
    'rabbit_host': parser.get('RABBITMQ', 'HOST'),
    'rabbit_port': int(parser.get('RABBITMQ', 'PORT')),
    'rabbit_username': parser.get('RABBITMQ', 'USERNAME'),
    'rabbit_password': parser.get('RABBITMQ', 'PASSWORD')
}

ckan_config = {
    'ckan_address': parser.get('CKAN', 'ADDRESS'),
    'ckan_apikey': parser.get('CKAN', 'APIKEY')
}
