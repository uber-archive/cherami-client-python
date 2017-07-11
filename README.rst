===============================
Cherami Client For Python
===============================

Python client library for publishing/consuming messages to/from `Cherami <https://github.com/uber/cherami-server>`_.

Installation
------------

``pip install cherami-client``


Running Tests
-------------

``make test``


Usage
-----

First, create and edit the ``.yaml`` file under ``./config``

See Example:
::
        cat ./config/test.yaml

Set the clay environment variable:
::
        export CLAY_CONFIG=./config/test.yaml

Run the example client:
::
        python ./example/example_publisher,py
        python ./example/example_consumer.py

