FROM continuumio/miniconda3:4.8.2-alpine

USER root

ENV PATH="/opt/conda/bin:$PATH"

RUN apk update --no-cache && apk add --no-cache --virtual .build-deps git openjdk8

COPY /granule_ingester /sdap/granule_ingester
COPY /setup.py /sdap/setup.py
COPY /requirements.txt /sdap/requirements.txt
COPY /conda-requirements.txt /sdap/conda-requirements.txt
COPY /docker/install_nexusproto.sh /install_nexusproto.sh
COPY /docker/entrypoint.sh /entrypoint.sh

RUN ./install_nexusproto.sh
RUN cd /sdap && python setup.py install

RUN apk del .build-deps

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]