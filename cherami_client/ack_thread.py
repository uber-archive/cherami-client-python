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
from threading import Thread,Event
from Queue import Empty

from cherami_client.lib import util, cherami
from cherami_client.ack_message_result import AckMessageResult


class AckThread(Thread):
    def __init__(self, tchannel, headers, logger, ack_queue, timeout_seconds):
        Thread.__init__(self)
        self.tchannel = tchannel
        self.headers = headers
        self.logger = logger
        self.ack_queue = ack_queue
        self.timeout_seconds = timeout_seconds
        self.stop_signal = Event()

    def stop(self):
        self.stop_signal.set()

    def run(self):
        while not self.stop_signal.is_set():
            try:
                hostport = None
                try:
                    is_ack, delivery_token, callback = self.ack_queue.get(block=True,timeout=self.timeout_seconds)
                    hostport = util.get_hostport_from_delivery_token(delivery_token)
                    util.stats_count(self.tchannel.name, 'consumer_ack_queue.dequeue', hostport, 1)
                except Empty:
                    continue

                ack_id = util.get_ack_id_from_delivery_token(delivery_token)
                request = cherami.AckMessagesRequest(ackIds=[ack_id] if is_ack else [],
                                                     nackIds=[ack_id] if not is_ack else [])

                util.execute_output_host(tchannel=self.tchannel,
                                         headers=self.headers,
                                         hostport=hostport,
                                         timeout=self.timeout_seconds,
                                         method_name='ackMessages',
                                         request=request)

                callback(AckMessageResult(call_success=True,
                                          is_ack=is_ack,
                                          delivery_token=delivery_token,
                                          error_msg=None))

            except Exception as e:
                self.logger.info({
                    'msg': 'error ack msg from output host',
                    'hostport': hostport,
                    'ack id': ack_id,
                    'is ack': is_ack,
                    'traceback': traceback.format_exc(),
                    'exception': str(e)
                })
                callback(AckMessageResult(call_success=False,
                                          is_ack=is_ack,
                                          delivery_token=delivery_token,
                                          error_msg=str(e)))
