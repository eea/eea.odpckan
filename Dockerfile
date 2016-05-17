FROM python:2-slim

MAINTAINER "European Environment Agency (EEA): IDM2 A-Team" <eea-edw-a-team-alerts@googlegroups.com>

#install git and chaperone
RUN apt-get update && \
    apt-get install -y python3-pip git && \
    pip3 install chaperone

#create group and user
RUN groupadd -g 999 odpckan && \
    useradd -g 999 -u 999 -m -s /bin/bash odpckan

COPY . /eea.odpckan/

RUN pip install -r /eea.odpckan/requirements.txt
RUN chown -R odpckan:odpckan /eea.odpckan

#setup chaperone
RUN mkdir -p /etc/chaperone.d
COPY chaperone.conf /etc/chaperone.d/chaperone.conf

USER odpckan

ENTRYPOINT ["/usr/local/bin/chaperone"]
CMD ["--user", "odpckan"]
