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

import threading
import traceback
from datetime import datetime
from six.moves.queue import Empty

from cherami_client.lib import cherami, cherami_input, util


class PublisherThread(threading.Thread):
    def __init__(self,
                 path,
                 task_queue,
                 tchannel,
                 hostport,
                 headers,
                 timeout_seconds,
                 checksum_option):
        threading.Thread.__init__(self)
        self.path = path
        self.task_queue = task_queue
        self.tchannel = tchannel
        self.hostport = hostport
        self.headers = headers
        self.timeout_seconds = timeout_seconds
        self.checksum_option = checksum_option
        self.stop_signal = threading.Event()
        self.thread_start_time = datetime.now()

    def stop(self):
        self.stop_signal.set()

    def run(self):
        while not self.stop_signal.is_set():
            try:
                # remove from queue regardless
                msg, callback = self.task_queue.get(block=True, timeout=5)
                self.task_queue.task_done()

                if self.checksum_option == cherami.ChecksumOption.CRC32IEEE:
                    msg.crc32IEEEDataChecksum = util.calc_crc(msg.data, self.checksum_option)
                elif self.checksum_option == cherami.ChecksumOption.MD5:
                    msg.md5DataChecksum = util.calc_crc(msg.data, self.checksum_option)

                request = cherami_input.PutMessageBatchRequest(
                   destinationPath=self.path,
                   messages=[msg])
                batch_result = util.execute_input_host(tchannel=self.tchannel,
                                                       headers=self.headers,
                                                       hostport=self.hostport,
                                                       timeout=self.timeout_seconds,
                                                       method_name='putMessageBatch',
                                                       request=request)

                if not callable(callback):
                    continue
                if batch_result and batch_result.successMessages and len(batch_result.successMessages) != 0:  # noqa
                    callback(batch_result.successMessages[0])
                    continue
                if batch_result and batch_result.failedMessages and len(batch_result.failedMessages) != 0:  # noqa
                    callback(batch_result.failedMessages[0])
                    continue

                # fallback: somehow no result received
                callback(util.create_failed_message_ack(msg.id, 'sender gets no result from input'))

            except Empty:
                pass

            except Exception:
                if msg and callable(callback):
                    failure_msg = 'traceback:{0}, hostport:{1}, thread start time:{2}'\
                                    .format(traceback.format_exc(),
                                            self.hostport,
                                            str(self.thread_start_time))
                    callback(util.create_failed_message_ack(msg.id, failure_msg))
