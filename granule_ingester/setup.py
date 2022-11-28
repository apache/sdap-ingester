from subprocess import check_call, CalledProcessError

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    pip_requirements = f.readlines()

with open('../VERSION.txt', 'r') as f:
    __version__ = f.readline()

try:
    check_call(['conda', 'install', '-y', '-c', 'conda-forge', '--file', 'conda-requirements.txt'])
except (CalledProcessError, IOError) as e:
    raise EnvironmentError("Error installing conda packages", e)

setup(
    name='sdap_granule_ingester',
    version=__version__,
    url="https://github.com/apache/incubator-sdap-ingester",
    author="dev@sdap.apache.org",
    author_email="dev@sdap.apache.org",
    description="Python modules that can be used for NEXUS ingest.",
    install_requires=pip_requirements,
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "scripts"]),
    test_suite="tests",
    platforms='any',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
    ]
)
