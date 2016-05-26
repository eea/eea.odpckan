FROM python:2-slim

MAINTAINER "European Environment Agency (EEA): IDM2 A-Team" <eea-edw-a-team-alerts@googlegroups.com>

ENV RABBITMQ_HOST=10.128.0.22 \
    RABBITMQ_PORT=5672 \
    CKAN_ADDRESS=https://open-data.europa.eu/en/data \
    SERVICES_EEA=http://www.eea.europa.eu/data-and-maps/data \
    SERVICES_SDS=http://semantic.eea.europa.eu/sparql \
    SERVICES_ODP=https://open-data.europa.eu/en/data/publisher/eea \

#install git and chaperone
RUN apt-get update && \
    apt-get install -y python3-pip git && \
    pip3 install chaperone

#create group and user
RUN groupadd -g 999 odpckan && \
    useradd -g 999 -u 999 -m -s /bin/bash odpckan

COPY . /eea.odpckan/

RUN pip install -r /eea.odpckan/app/requirements.txt
RUN chown -R odpckan:odpckan /eea.odpckan

#setup chaperone
RUN mkdir -p /etc/chaperone.d
COPY chaperone.conf /etc/chaperone.d/chaperone.conf

USER odpckan

ENTRYPOINT ["/usr/local/bin/chaperone"]
CMD ["--user", "odpckan"]
