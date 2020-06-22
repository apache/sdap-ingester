FROM python:3

RUN apt-get update && apt-get install -y apt-transport-https gnupg2
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update && apt-get install -y kubectl

COPY /collection_manager /collection_manager/collection_manager
COPY /setup.py /collection_manager/setup.py
COPY /requirements.txt /collection_manager/requirements.txt
COPY /README.md /collection_manager/README.md
COPY /containers/docker/entrypoint.sh /entrypoint.sh

RUN cd /collection_manager && python setup.py install

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
