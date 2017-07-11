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

from __future__ import absolute_import

import traceback
from threading import Thread, Event
from Queue import Full

from cherami_client.lib import util
from cherami_client.lib import cherami


class ConsumerThread(Thread):
    def __init__(self, tchannel, headers, logger, msg_queue, hostport, path, consumer_group_name, timeout_seconds, msg_batch_size):
        Thread.__init__(self)
        self.tchannel = tchannel
        self.headers = headers
        self.logger = logger
        self.msg_queue = msg_queue
        self.hostport = hostport
        self.path = path
        self.consumer_group_name = consumer_group_name
        self.timeout_seconds = timeout_seconds
        self.msg_batch_size = msg_batch_size
        self.stop_signal = Event()

    def stop(self):
        self.stop_signal.set()

    def run(self):
        request = cherami.ReceiveMessageBatchRequest(destinationPath=self.path,
                                                     consumerGroupName=self.consumer_group_name,
                                                     maxNumberOfMessages=self.msg_batch_size,
                                                     receiveTimeout=max(1, self.timeout_seconds - 1)
                                                     )
        while not self.stop_signal.is_set():
            # possible optimization: if we don't have enough capacity in the queue, backoff for a bit before pulling from Cherami again
            try:
                result = util.execute_output_host(tchannel=self.tchannel,
                                                  headers=self.headers,
                                                  hostport=self.hostport,
                                                  timeout=self.timeout_seconds,
                                                  method_name='receiveMessageBatch',
                                                  request=request)
                util.stats_count(self.tchannel.name, 'receiveMessageBatch.messages', self.hostport, len(result.messages))

                for msg in result.messages:
                    # if the queue is full, keep trying until there's free slot, or the thread has been shutdown
                    while not self.stop_signal.is_set():
                        try:
                            self.msg_queue.put((util.create_delivery_token(msg.ackId, self.hostport),msg), block=True, timeout=5)
                            util.stats_count(self.tchannel.name, 'consumer_msg_queue.enqueue', self.hostport, 1)
                            break
                        except Full:
                            pass
            except Exception as e:
                self.logger.info({
                    'msg': 'error receiving msg from output host',
                    'hostport': self.hostport,
                    'traceback': traceback.format_exc(),
                    'exception': str(e)
                })
