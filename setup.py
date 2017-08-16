from __future__ import print_function

from setuptools import setup, find_packages
from pip.req import parse_requirements


__version__ = '1.0.2'
name = 'cherami-client'


def read_long_description(filename="README.rst"):
    with open(filename) as f:
        return f.read().strip()


setup(
    name=name,
    version=__version__,
    author='Wei Han',
    author_email='weihan@uber.com',
    url='https://github.com/uber/cherami-client-python',
    description='Cherami Python Client Library',
    packages=find_packages(exclude=['tests', 'demo', 'tests.*']),
    include_package_data=True,
    package_data={
        name: [
            'idl/*.thrift',
        ],
    },
    license='MIT',
    keywords='cherami python client',
    long_description=read_long_description(),
    install_requires=[
        'tchannel>=1.0.1',
        'zest.releaser>=6.0,<7.0',
        'crcmod',
        'clay-flask',
        'PyYAML',
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='tests',
    extras_require={
        'tests': [
            'coverage==4.0.3',
            'pytest==2.8.7',
            'pytest-cov',
            'pytest-ipdb==0.1-prerelease2',
            'pytest-tornado',
            'py==1.4.31',           # via pytest
            'flake8==2.5.4',
            'pyflakes==1.0.0',      # via flake8
            'pep8==1.7.0',          # via flake8
            'mccabe==0.4.0',        # via flake8
            'mock==2.0.0',
            'six==1.10.0',          # via mock
            'pbr==1.9.1',           # via mock
            'funcsigs==1.0.2',      # via mock
        ],
    }
)
