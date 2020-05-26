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
    scripts=['bin/run_collections',
             'bin/run_single_collection',
             'bin/run_granules'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
    ],
    python_requires='>=3.6',
    include_package_data=True,
    data_files=[('.sdap_ingest_manager',
                 ['sdap_ingest_manager/ingestion_order_executor/resources/config/credentials.json.template',
                  'sdap_ingest_manager/ingestion_order_executor/resources/config/sdap_ingest_manager.ini.default',
                  'sdap_ingest_manager/ingestion_order_executor/resources/config/collections.yml.example']
                 ),
                ('.sdap_ingest_manager/resources/',
                 ['sdap_ingest_manager/ingestion_order_executor/resources/dataset_config_template.yml',
                  'sdap_ingest_manager/granule_ingester/resources/connection-config.yml',
                  'sdap_ingest_manager/granule_ingester/resources/job-deployment-template.yml']
                 ),
                ('.sdap_ingest_manager/resources/test/data/avhrr_oi/',
                 ['sdap_ingest_manager/ingestion_order_executor/test/data/avhrr_oi/20151101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc',
                  'sdap_ingest_manager/ingestion_order_executor/test/data/avhrr_oi/20151102120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'])
            ],
    install_requires=[
        "google-api-python-client>=1.7",
        "google-auth-oauthlib>=0.4",
        "pystache>=0.5",
        "pyyaml",
        "pysolr>=3.8"
    ]
)

post_install_message()
