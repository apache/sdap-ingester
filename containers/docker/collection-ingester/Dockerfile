FROM python:3

# Add kubernetes client to create other pods (ingester)
RUN apt-get update && apt-get install -y apt-transport-https gnupg2
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update && apt-get install -y kubectl

RUN pip install https://github.com/tloubrieu-jpl/incubator-sdap-nexus-ingestion-manager/releases/download/0.4.0+dev/sdap_ingest_manager-0.4.0+dev-py3-none-any.whl

CMD bash
