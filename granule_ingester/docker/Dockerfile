# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM eclipse-temurin:8

USER root

COPY --chmod=755 granule_ingester/docker/install_python.sh /install_python.sh
RUN /install_python.sh
RUN ln -s /opt/python/3.11.15/bin/python3.11 /opt/python/3.11.15/bin/python

RUN apt-get update && \
    apt-get install --no-install-recommends -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/python/3.11.15/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PYTHON=/opt/python/3.11.15/bin/python3.11

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY common/pyproject.toml common/
COPY collection_manager/pyproject.toml collection_manager/
COPY config_operator/pyproject.toml config_operator/
COPY granule_ingester/pyproject.toml granule_ingester/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package granule_ingester

COPY common/ common/
COPY granule_ingester/ granule_ingester/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package granule_ingester

# The syncs above used UV_PYTHON to build the venv from the compiled base
# interpreter. From here on, point uv (and gradle's `python`) at the venv so
# install_nexusproto.sh installs nexusproto INTO /app/.venv rather than the
# base /opt/python interpreter that the runtime never uses.
ENV VIRTUAL_ENV="/app/.venv" \
    PATH="/app/.venv/bin:$PATH" \
    UV_PYTHON="/app/.venv/bin/python"

ARG BUILD_NEXUSPROTO
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master

COPY --chmod=755 granule_ingester/docker/install_nexusproto.sh /app/install_nexusproto.sh
RUN ./install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH

COPY granule_ingester/docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
