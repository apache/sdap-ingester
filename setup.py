import setuptools
import os
import subprocess
import sys
import re

PACKAGE_NAME = "sdap_ingest_manager"


def post_install_message():
    try:
        from tabulate import tabulate
    except ImportError:
        subprocess.call([sys.executable, "-m", "pip", "install", 'tabulate'])
    finally:
        from tabulate import tabulate

    path_to_configuration_files = os.path.join(sys.prefix, f".{PACKAGE_NAME}")
    message = f"Now, create configuration files in \n" \
              f"***{path_to_configuration_files}*** \n" \
              f" Use templates and examples provided there"
    print(tabulate([[message]]))


with open("./sdap_ingest_manager/__init__.py") as fi:
    result = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fi.read())
version = result.group(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
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
    data_files=[('.sdap_ingest_manager/resources/', ['sdap_ingest_manager/resources/dataset_config_template.yml'])],
    install_requires=pip_requirements,
    entry_points={
        'config-operator': ['summary=sdap_ingest_manager.config_operator:main'],
        'collection-ingester': ['summary=sdap_ingest_manager.service:main'],
    },

)

post_install_message()
