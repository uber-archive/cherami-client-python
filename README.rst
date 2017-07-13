.. image:: https://travis-ci.org/uber/cherami-client-python.svg?branch=master
    :target: https://travis-ci.org/uber/cherami-client-python

.. image:: https://coveralls.io/repos/github/uber/cherami-client-python/badge.svg?branch=master
    :target: https://coveralls.io/github/uber/cherami-client-python?branch=master

.. image:: https://badge.fury.io/py/cherami-client.svg
    :target: https://badge.fury.io/py/cherami-client

===============================
Cherami Client For Python
===============================

Python client library for publishing/consuming messages to/from `Cherami <https://github.com/uber/cherami-server>`_.

Installation
------------

``pip install cherami-client``

Usage
-----

Create and edit the ``.yaml`` file under ``./config``

See Example:
::
        cat ./config/test.yaml

Set the clay environment variable:
::
        export CLAY_CONFIG=./config/test.yaml

Run the example client:
::
        python ./demo/example_publisher.py
        python ./demo/example_consumer.py

Contributing
------------
We'd love your help in making Cherami great. If you find a bug or need a new feature, open an issue and we will respond as fast as we can.
If you want to implement new feature(s) and/or fix bug(s) yourself, open a pull request with the appropriate unit tests and we will merge it after review.

Note: All contributors also need to fill out the `Uber Contributor License Agreement <http://t.uber.com/cla>`_ before we can merge in any of your changes.

Documentation
-------------
Interested in learning more about Cherami? Read the blog post: `eng.uber.com/cherami <https://eng.uber.com/cherami/>`_

License
-------
MIT License, please see `LICENSE <https://github.com/uber/cherami-client-python/blob/master/LICENSE>`_ for details.
