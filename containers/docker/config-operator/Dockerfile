FROM python:3

COPY /sdap_ingest_manager /sdap_ingest_manager/sdap_ingest_manager
COPY /setup.py /sdap_ingest_manager/setup.py
COPY /requirements.txt /sdap_ingest_manager/requirements.txt
COPY /README.md /sdap_ingest_manager/README.md

RUN cd /sdap_ingest_manager && pip install .

CMD bash
