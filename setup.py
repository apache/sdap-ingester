import setuptools
import os
import subprocess
import sys
import site


def post_install_message():
    try:
        from tabulate import tabulate
    except ImportError:
        subprocess.call([sys.executable, "-m", "pip", "install", 'tabulate'])
    finally:
        from tabulate import tabulate

    path_to_configuration_files = os.path.expanduser(f"~/.{PACKAGE_NAME}")
    message = f"Now, create configuration files in \n" \
              f"***{path_to_configuration_files}*** \n" \
              f" Use templates and examples provided there"
    print(tabulate([[message]]))


with open("README.md", "r") as fh:
    long_description = fh.read()

PACKAGE_NAME = "sdap_ingest_manager"

setuptools.setup(
    name=PACKAGE_NAME, # Replace with your own username
    version="0.0.1",
    author="Apache - SDAP",
    author_email="dev@sdap.apache.org",
    description="a helper to ingest data in sdap",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tloubrieu-jpl/incubator-sdap-nexus-ingestion-manager",
    packages=setuptools.find_packages(),
    scripts=['bin/run_collections',
             'bin/run_granule'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        'Development Status :: 1 - Pre-Alpha',
    ],
    python_requires='>=3.6',
    include_package_data=True,
    data_files=[(os.path.expanduser('~/.sdap_ingest_manager'),
                 ['sdap_ingest_manager/sdap_ingest_manager/resources/config/credentials.json.template',
                  'sdap_ingest_manager/sdap_ingest_manager/resources/config/sdap_ingest_manager.ini.example']
                 )],
    install_requires=[
        "google-api-python-client>=1.7",
        "google-auth-oauthlib==0.4.1",
    ]
)

if len(sys.argv) >= 2 and sys.argv[1] == "install":
    post_install_message()