FROM continuumio/miniconda3:4.8.2-alpine

USER root

ENV PATH="/opt/conda/bin:$PATH"

RUN apk update --no-cache && apk add --no-cache --virtual .build-deps git openjdk8

COPY common /common
COPY granule_ingester/granule_ingester /sdap/granule_ingester
COPY granule_ingester/setup.py /sdap/setup.py
COPY granule_ingester/requirements.txt /sdap/requirements.txt
COPY granule_ingester/conda-requirements.txt /sdap/conda-requirements.txt
COPY granule_ingester/docker/install_nexusproto.sh /install_nexusproto.sh
COPY granule_ingester/docker/entrypoint.sh /entrypoint.sh

RUN ./install_nexusproto.sh
RUN cd /common && python setup.py install
RUN cd /sdap && python setup.py install

RUN pip install boto3==1.16.10

RUN apk del .build-deps

USER 1001
ENV OPENBLAS_NUM_THREADS=1
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
