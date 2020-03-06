import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sdap_ingest_manager", # Replace with your own username
    version="0.0.1",
    author="Apache - SDAP",
    author_email="dev@sdap.apache.org",
    description="a helper to ingest data in sdap",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/incubator-sdap-nexus",
    packages=setuptools.find_packages(),
    scripts=['bin/run/run_collections'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        'Development Status :: 1 - Pre-Alpha',
    ],
    python_requires='>=3.7',
)