FROM python:3

RUN apt-get update && apt-get install -y apt-transport-https gnupg2
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update && apt-get install -y kubectl

COPY common /common
COPY collection_manager/collection_manager /collection_manager/collection_manager
COPY collection_manager/setup.py /collection_manager/setup.py
COPY collection_manager/requirements.txt /collection_manager/requirements.txt
COPY collection_manager/README.md /collection_manager/README.md
COPY collection_manager/docker/entrypoint.sh /entrypoint.sh

RUN cd /common && python setup.py install
RUN cd /collection_manager && python setup.py install

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
