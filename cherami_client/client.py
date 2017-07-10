# Copyright (c) 2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import socket

from tchannel.sync import TChannel as TChannelSyncClient
from cherami_client.lib import util
from cherami_client import publisher, consumer


class Client(object):

    tchannel = None
    headers = None
    timeout_seconds = 30

    # reconfigure_interval_seconds: this parameter controls how frequent we try to get the latest hosts
    # that are serving the destination or consumer group in our background thread
    #
    # deployment_str: controls the deployment the client will connect to.
    # use 'dev' to connect to dev server
    # use 'prod' to connect to production
    # use 'staging' or 'staging2' to connect to staging/staging2
    #
    # hyperbahn_host: the path to the hyperbahn host file
    #
    # By default client will connect to cherami via hyperbahn. If you want to connect to cherami via a
    # specific ip/port(for example during local test), you can create a tchannel object with the ip/port
    # as known peers
    # For example:
    # tchannel = TChannelSyncClient(name='my_service', known_peers=['172.17.0.2:4922'])
    # client = Client(tchannel, logger)
    def __init__(self,
                 tchannel,
                 logger,
                 client_name=None,
                 headers={},
                 timeout_seconds=30,
                 reconfigure_interval_seconds=10,
                 deployment_str='prod',
                 hyperbahn_host='',
                 ):
        self.logger = logger
        self.headers = headers
        self.deployment_str = deployment_str
        self.headers['user-name'] = util.get_username()
        self.headers['host-name'] = socket.gethostname()
        self.timeout_seconds = timeout_seconds
        self.reconfigure_interval_seconds = reconfigure_interval_seconds

        if not tchannel:
            if not client_name:
                raise Exception("Client name is needed when tchannel not provided")
            elif not hyperbahn_host:
                raise Exception("Hyperbahn host is needed when tchannel not provided")
            else:
                self.tchannel = TChannelSyncClient(name=client_name)
                self.tchannel.advertise(router_file=hyperbahn_host)
        else:
            self.tchannel = tchannel

    # close the client connection
    def close(self):
        pass

    # create a consumer
    # Note consumer object should be a singleton
    # pre_fetch_count: This controls how many messages we can pre-fetch in total
    # ack_message_buffer_size: This controls the ack messages buffer size.i.e.count of pending ack messages
    # ack_message_thread_count: This controls how many threads we can have to send ack messages to Cherami.
    def create_consumer(
            self,
            path,
            consumer_group_name,
            pre_fetch_count=50,
            ack_message_buffer_size=50,
            ack_message_thread_count=4,
            ):
        return consumer.Consumer(
            logger=self.logger,
            deployment_str=self.deployment_str,
            path=path,
            consumer_group_name=consumer_group_name,
            tchannel=self.tchannel,
            headers=self.headers,
            pre_fetch_count=pre_fetch_count,
            timeout_seconds=self.timeout_seconds,
            ack_message_buffer_size=ack_message_buffer_size,
            ack_message_thread_count=ack_message_thread_count,
            reconfigure_interval_seconds=self.reconfigure_interval_seconds,
        )

    # create a publisher
    # Note publisher object should be a singleton
    def create_publisher(self, path):
        if not path:
            raise Exception("Path is needed")
        return publisher.Publisher(
            logger=self.logger,
            path=path,
            deployment_str=self.deployment_str,
            tchannel=self.tchannel,
            headers=self.headers,
            timeout_seconds=self.timeout_seconds,
            reconfigure_interval_seconds=self.reconfigure_interval_seconds
        )

    def create_destination(self, create_destination_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'createDestination', create_destination_request)

    def read_destination(self, read_destination_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'readDestination', read_destination_request)

    def create_consumer_group(self, create_consumer_group_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'createConsumerGroup', create_consumer_group_request)

    def read_consumer_group(self, read_consumer_group_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'readConsumerGroup', read_consumer_group_request)

    def purge_DLQ_for_consumer_group(self, purge_DLQ_for_consumer_group_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'purgeDLQForConsumerGroup', purge_DLQ_for_consumer_group_request)

    def merge_DLQ_for_consumer_group(self, merge_DLQ_for_consumer_group_request):
        return util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'mergeDLQForConsumerGroup', merge_DLQ_for_consumer_group_request)
