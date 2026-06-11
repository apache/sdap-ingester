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

FROM python:3.11-slim

# GitPython (used by RemoteGitConfig) shells out to the git binary at import
# time; the slim base image does not ship it, so install it explicitly.
RUN apt-get update && \
    apt-get install --no-install-recommends -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY common/pyproject.toml common/
COPY collection_manager/pyproject.toml collection_manager/
COPY config_operator/pyproject.toml config_operator/
COPY granule_ingester/pyproject.toml granule_ingester/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package config_operator

COPY config_operator/ config_operator/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package config_operator

ENV PATH="/app/.venv/bin:$PATH"
CMD ["kopf", "run", "/app/config_operator/config_operator/main.py", "--verbose"]
