from __future__ import print_function

import sys
import collections

from setuptools import setup, find_packages
from pip.req import parse_requirements
from setuptools.command.test import test as TestCommand


__version__ = '1.0.0'
name = 'cherami_client'

dependency_links = []
install_requires = []

ReqOpts = collections.namedtuple('ReqOpts',
                                 ['skip_requirements_regex', 'default_vcs'])

opts = ReqOpts(None, 'git')

for ir in parse_requirements('requirements.txt', options=opts, session=False):
    if ir is not None:
        if ir.url is not None:
            dependency_links.append(str(ir.url))
        if ir.req is not None:
            install_requires.append(str(ir.req))


def read_long_description(filename="README.rst"):
    with open(filename) as f:
        return f.read().strip()


def requirements(filename="requirements.txt"):
    with open(filename) as f:
        return f.readlines()


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    name=name,
    version=__version__,
    url='https://github.com/uber/cherami-client-python',
    description='Cherami Python Client Library',
    packages=find_packages(exclude=['tests', 'demo', 'tests.*']),
    include_package_data=True,
    package_data={
        name: [
            'idl/*.thrift',
        ],
    },
    cmdclass={'test': PyTest},
    license='MIT',
    keywords='cherami python client',
    long_description=read_long_description(),
    install_requires=install_requires,
    dependency_links=dependency_links,
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ],
)