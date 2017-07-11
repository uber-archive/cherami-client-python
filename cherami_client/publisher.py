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

import Queue
import threading

from cherami_client.lib import cherami, cherami_input, util
from cherami_client.publisher_thread import PublisherThread
from cherami_client.reconfigure_thread import ReconfigureThread


class Publisher(object):

    def __init__(self,
                 logger,
                 path,
                 tchannel,
                 deployment_str,
                 headers,
                 timeout_seconds,
                 reconfigure_interval_seconds):
        self.logger = logger
        self.path = path
        self.tchannel = tchannel
        self.deployment_str = deployment_str
        self.headers = headers
        self.timeout_seconds = timeout_seconds
        self.task_queue = Queue.Queue()
        self.workers = {}
        self.reconfigure_signal = threading.Event()
        self.reconfigure_interval_seconds = reconfigure_interval_seconds
        self.reconfigure_thread = None

    def _reconfigure(self):
        self.logger.info('publisher reconfiguration started')
        result = util.execute_frontend(
            self.tchannel, self.deployment_str, self.headers, self.timeout_seconds, 'readPublisherOptions',
            cherami.ReadPublisherOptionsRequest(
                path=self.path,
            ))

        hostAddresses = []
        for host_protocol in result.hostProtocols:
            if host_protocol.protocol == cherami.Protocol.TCHANNEL:
                hostAddresses = host_protocol.hostAddresses
                break

        if not hostAddresses:
            raise Exception("tchannel protocol is not supported by cherami server")

        host_connection_set = set(map(lambda h: util.get_connection_key(h), hostAddresses))
        existing_connection_set = set(self.workers.keys())
        missing_connection_set = host_connection_set - existing_connection_set
        extra_connection_set = existing_connection_set - host_connection_set

        # clean up
        for extra_conn in extra_connection_set:
            self.logger.info('cleaning up connection %s', extra_conn)
            self.workers[extra_conn].stop()
            del self.workers[extra_conn]

        # start up
        for missing_conn in missing_connection_set:
            self.logger.info('creating new connection %s', missing_conn)
            worker = PublisherThread(
                path=self.path,
                task_queue=self.task_queue,
                tchannel=self.tchannel,
                hostport=missing_conn,
                headers=self.headers,
                timeout_seconds=self.timeout_seconds,
                checksum_option=result.checksumOption
            )
            self.workers[missing_conn] = worker
            worker.start()

        self.logger.info('publisher reconfiguration succeeded')

    # open the publisher. If succeed, we can start to publish messages
    # Otherwise, we should retry opening (with backoff)
    def open(self):
        try:
            self._reconfigure()
            self.reconfigure_thread = ReconfigureThread(
                interval_seconds=self.reconfigure_interval_seconds,
                reconfigure_signal=self.reconfigure_signal,
                reconfigure_func=self._reconfigure,
                logger=self.logger,
            )
            self.reconfigure_thread.start()
        except Exception as e:
            self.logger.exception('Failed to open publisher: %s', e)
            self.close()
            raise e

    # close the publisher
    def close(self):
        if self.reconfigure_thread:
            self.reconfigure_thread.stop()
        for worker in self.workers.itervalues():
            worker.stop()

    # publish a message. Returns an ack(type is cherami.PutMessageAck)
    # the Status field of the ack indicates whether the publish was successful or not
    # id: an identifier client can use to identify messages \
    #     (cherami doesn't care about this field but just pass through)
    # data: message payload
    # user context: user specified context to pass through
    def publish(self, id, data, userContext={}):
        done_signal = threading.Event()
        result = []

        def done_callback(r):
            result.append(r)
            done_signal.set()

        # publish and later on wait
        self.publish_async(id, data, done_callback, userContext)

        done = done_signal.wait(self.timeout_seconds)
        if not done:
            return util.create_timeout_message_ack(id)
        if len(result) == 0:
            return util.create_failed_message_ack(id, 'unexpected: callback does not carry result')
        return result[0]

    # asynchronously publish a message.
    # A callback function needs to be provided(it expects a cherami.PutMessageAck object as parameter)
    def publish_async(self, id, data, callback, userContext={}):
        msg = cherami_input.PutMessage(
            id=id,
            delayMessageInSeconds=0,
            data=data,
            userContext=userContext
        )
        self.task_queue.put((msg, callback))
