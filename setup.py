from __future__ import print_function

from setuptools import setup, find_packages
from pip.req import parse_requirements


__version__ = '1.0.0'
name = 'cherami-client'


install_requires = []
test_requires = []


for r in parse_requirements('requirements.txt', session=False):
    install_requires.append(str(r.req))


for r in parse_requirements('requirements-test.txt', session=False):
    test_requires.append(str(r.req))


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
    install_requires=install_requires,
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
        'tests': test_requires,
    }
)