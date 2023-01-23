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

import setuptools
import re

PACKAGE_NAME = "config_operator"

with open(f'./{PACKAGE_NAME}/__init__.py') as fi:
    result = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fi.read())
version = result.group(1)


with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    requirements = f.readlines()

setuptools.setup(
    name=PACKAGE_NAME,
    version=version,
    author="Apache - SDAP",
    author_email="dev@sdap.apache.org",
    description="a service to synchronize git or local directory configuration with k8s configMap",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tloubrieu-jpl/incubator-sdap-nexus-ingestion-manager",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
    ],
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': ['config-operator=config_operator.config_operator:main']
    }
)
