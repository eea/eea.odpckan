# ODP CKAN

- read messages from the RabbitMQ service
- interrogate [SDS](http://semantic.eea.europa.eu) and retrieve full data about the specified datasets in JSON format
- updates the [EU Open Data Portal (ODP)](https://open-data.europa.eu/en/data/publisher/eea) using CKAN API

## Installation

Dependencies

- [Pika](https://pika.readthedocs.org/en/0.10.0/) a python client for RabbitMQ
- [ckanapi](https://github.com/ckan/ckanapi) a python client for [CKAN API](http://docs.ckan.org/en/latest/contents.html) to work with ODP

Clone the repository

    $ git clone https://github.com/eea/eea.odpckan.git
    $ cd eea.odpckan

Install all dependencies with pip command

    $ pip install -r requirements.txt

During the first time deployement, create and edit the secret file

    $ cp .secret.example .secret
    $ vim .secret
    $ # edit connection for both RabbitMQ and CKAN services

## Usage

work in progress

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
