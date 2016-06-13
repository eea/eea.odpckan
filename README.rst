==========================================
ODP CKAN - EU Open Data Portal CKAN client
==========================================

- read messages from the RabbitMQ service
- interrogate `SDS <http://semantic.eea.europa.eu>`_ and retrieve full data about the specified datasets in JSON format
- updates the `EU Open Data Portal (ODP) <https://open-data.europa.eu/en/data/publisher/eea>`_ using CKAN API

.. contents::

Base docker image
=================

 - `hub.docker.com <https://registry.hub.docker.com/u/eeacms/odpckan>`_

Source code
===========

  - `eea.odpckan <http://github.com/eea/eea.odpckan>`_

Usage via Docker
================

During the first time deployement, create and edit the .secret file, see the `.secret.example <.secret.example>`_::

    $ touch .secret
    $ vim .secret
    $ # edit connection for both RabbitMQ and CKAN services. see .secret.example

Start the odpckan client with the following command::

    $ sudo docker run -d -v /etc/localtime:/etc/localtime:ro -v ./.secret:/eea.odpckan/.secret:z docker.io/eeacms/odpckan -e RABBITMQ_HOST=http://rabbitmq.apps.eea.europa.eu -e RABBITMQ_PORT=5672 -e CKAN_ADDRESS=https://open-data.europa.eu/en/data -e SERVICES_EEA=http://www.eea.europa.eu/data-and-maps/data -e SERVICES_SDS=http://semantic.eea.europa.eu/sparql -e SERVICES_ODP=https://open-data.europa.eu/en/data/publisher/eea -e CKANCLIENT_INTERVAL="0 */3 * * *"

For docker-compose orchestration see `eea.docker.odpckan <https://github.com/eea/eea.docker.odpckan>`_.

Usage w/o Docker
================

Dependencies

- `Pika <https://pika.readthedocs.org/en/0.10.0/>`_ a python client for RabbitMQ
- `ckanapi <https://github.com/ckan/ckanapi>`_ a python client for [CKAN API](http://docs.ckan.org/en/latest/contents.html) to work with ODP
- `rdflib <https://github.com/RDFLib/rdflib/>`_ a python library for working with RDF
- `rdflib-jsonld <https://github.com/RDFLib/rdflib-jsonld>`_ JSON-LD parser and serializer plugins for RDFLib

Clone the repository::

    $ git clone https://github.com/eea/eea.odpckan.git
    $ cd eea.odpckan

Install all dependencies with pip command::

    $ pip install -r requirements.txt

During the first time deployement, create and edit the secret file::

    $ cp .secret.example .secret
    $ vim .secret
    $ # edit connection for both RabbitMQ and CKAN services

Example usage
=============

ODP CKAN entry point that will start consume all the messages from the queue and stops after. This command can be setup as a cron job.::

    $ python app/ckanclient.py

Inject test messages (default howmany = 1)::

    $ python app/proxy.py howmany

Query SDS (default url = http://www.eea.europa.eu/data-and-maps/data/eea-coastline-for-analysis-1) and print result::

    $ python app/sdsclient.py url

EEA main portal use case
========================

Information published on [EEA main portal](http://www.eea.europa.eu) is submitted to the [EU Open Data Portal](https://data.europa.eu).

![EEA ODP CKAN workflow diagram](https://raw.githubusercontent.com/eea/eea.odpckan/master/docs/EEA%20ODP%20CKAN%20-%20workflow%20diagram.png)

The workflow is described below:

- [EEA main portal](http://www.eea.europa.eu) - Plone CMS
  - content is published
  - CMS content rules are triggered and the following operations are performed:
    - a message is added in [RabbitMQ message broker](http://rabbitmq.apps.eea.europa.eu) queue, see example below
    - [SDS](http://semantic.eea.europa.eu) is pinged to update its harvested content

- [EEA ODP CKAN](https://github.com/eea/eea.odpckan/tree/master/app) client
  - CKAN client is triggered periodically via a cron job
  - CKAN client connect to  [RabbitMQ message broker](http://rabbitmq.apps.eea.europa.eu) and consumes all the messages from the “odp_queue” queue performing following operations
    - dataset is identified
    - dataset’s metadata is extracted from [SDS](http://semantic.eea.europa.eu)
    - using CKAN API, [OPD](http://data.europa.eu/euodp) is updated
    - if issues occur during message processing the message is re queued

- [EEA ODP CKAN](https://github.com/eea/eea.odpckan/tree/master/app) client - bulk update operation 
    - is triggered periodically via a cron job
    - it reads all the datasets from the [SDS](http://semantic.eea.europa.eu)
    - generates update messages in the [RabbitMQ message broker](http://rabbitmq.apps.eea.europa.eu), one message per dataset found

RabbitMQ message example
------------------------

Message::

    $ update|http://www.eea.europa.eu/data-and-maps/data/eea-coastline-for-analysis-1 |eea-coastline-for-analysis-1

Message structure::

    $ action|url|identifier

Action(s)::

    $ create/update/delete

Copyright and license
=====================

The Initial Owner of the Original Code is European Environment Agency (EEA).
All Rights Reserved.

The Original Code is free software;
you can redistribute it and/or modify it under the terms of the GNU
General Public License as published by the Free Software Foundation;
either version 2 of the License, or (at your option) any later
version.

Funding
=======

`European Environment Agency (EU) <http://eea.europa.eu>`_
