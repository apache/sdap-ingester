import setuptools
import os
import subprocess
import sys
import re

PACKAGE_NAME = "sdap_collection_manager"

with open("./sdap_ingest_manager/__init__.py") as fi:
    result = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fi.read())
version = result.group(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    pip_requirements = f.readlines()

setuptools.setup(
    name=PACKAGE_NAME,
    version=version,
    author="Apache - SDAP",
    author_email="dev@sdap.apache.org",
    description="a helper to ingest data in sdap",
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
    data_files=[('.collection_manager/resources/', ['collection_manager/resources/dataset_config_template.yml'])],
    install_requires=pip_requirements
)
