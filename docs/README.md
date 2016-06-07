# ODP CKAN - EU Open Data Portal CKAN client

- read messages from the RabbitMQ service
- interrogate [SDS](http://semantic.eea.europa.eu) and retrieve full data about the specified datasets in JSON format
- updates the [EU Open Data Portal (ODP)](https://open-data.europa.eu/en/data/publisher/eea) using CKAN API

## Base docker image

 - [hub.docker.com](https://registry.hub.docker.com/u/eeacms/odpckan)

## Source code

  - [eea.odpckan](http://github.com/eea/eea.odpckan)

## Usage via Docker

During the first time deployement, create and edit the .secret file, see the [.secret.example](.secret.example)

    $ touch .secret
    $ vim .secret
    $ # edit connection for both RabbitMQ and CKAN services. see .secret.example

Start the odpckan client with the following command:

    $ sudo docker run -d -v /etc/localtime:/etc/localtime:ro -v ./.secret:/eea.odpckan/.secret:z docker.io/eeacms/odpckan -e RABBITMQ_HOST=http://rabbitmq.apps.eea.europa.eu -e RABBITMQ_PORT=5672 -e CKAN_ADDRESS=https://open-data.europa.eu/en/data -e SERVICES_EEA=http://www.eea.europa.eu/data-and-maps/data -e SERVICES_SDS=http://semantic.eea.europa.eu/sparql -e SERVICES_ODP=https://open-data.europa.eu/en/data/publisher/eea -e CKANCLIENT_INTERVAL="0 */3 * * *"

For docker-compose orchestration see [eea.docker.odpckan](https://github.com/eea/eea.docker.odpckan).                                                              

## Usage w/o Docker

Dependencies

- [Pika](https://pika.readthedocs.org/en/0.10.0/) a python client for RabbitMQ
- [ckanapi](https://github.com/ckan/ckanapi) a python client for [CKAN API](http://docs.ckan.org/en/latest/contents.html) to work with ODP
- [rdflib](https://github.com/RDFLib/rdflib/) a python library for working with RDF
- [rdflib-jsonld](https://github.com/RDFLib/rdflib-jsonld) JSON-LD parser and serializer plugins for RDFLib

Clone the repository

    $ git clone https://github.com/eea/eea.odpckan.git
    $ cd eea.odpckan

Install all dependencies with pip command

    $ pip install -r requirements.txt

During the first time deployement, create and edit the secret file

    $ cp .secret.example .secret
    $ vim .secret
    $ # edit connection for both RabbitMQ and CKAN services

## Example usage

ODP CKAN entry point that will start consume all the messages from the queue and stops after. This command can be setup as a cron job.

    $ python app/ckanclient.py

Inject test messages (default howmany = 1)

    $ python app/proxy.py howmany

Query SDS (default url = http://www.eea.europa.eu/data-and-maps/data/eea-coastline-for-analysis-1) and print result

    $ python app/sdsclient.py url

## Copyright and license

The Initial Owner of the Original Code is European Environment Agency (EEA).
All Rights Reserved.

The Original Code is free software;
you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation;
either version 2 of the License, or (at your option) any later
version.

## Funding

[European Environment Agency (EU)](http://eea.europa.eu)
