import re

import setuptools

PACKAGE_NAME = "sdap_ingester_common"

setuptools.setup(
    name=PACKAGE_NAME,
    author="Apache - SDAP",
    author_email="dev@sdap.apache.org",
    description="a module of common functions for the sdap ingester components",
    url="https://github.com/apache/incubator-sdap-ingester",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
    ],
    python_requires='>=3.7',
    include_package_data=True
)
