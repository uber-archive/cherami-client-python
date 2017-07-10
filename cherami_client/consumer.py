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

import time

from threading import Event
from Queue import Queue, Empty, Full

from clay import stats
from cherami_client.lib import cherami, util
from cherami_client.consumer_thread import ConsumerThread
from cherami_client.ack_thread import AckThread
from cherami_client.reconfigure_thread import ReconfigureThread
from cherami_client.ack_message_result import AckMessageResult


class Consumer(object):
    def __init__(self,
                 logger,
                 deployment_str,
                 path,
                 consumer_group_name,
                 tchannel,
                 headers,
                 pre_fetch_count,
                 timeout_seconds,
                 ack_message_buffer_size,
                 ack_message_thread_count,
                 reconfigure_interval_seconds,
                 ):
        self.logger = logger
        self.deployment_str = deployment_str
        self.path = path
        self.consumer_group_name = consumer_group_name
        self.tchannel = tchannel
        self.headers = headers
        self.pre_fetch_count = pre_fetch_count
        self.msg_queue = Queue(pre_fetch_count)
        self.msg_batch_size = max(pre_fetch_count/10, 1)
        self.timeout_seconds = timeout_seconds
        self.consumer_threads = {}
        self.ack_queue = Queue(ack_message_buffer_size)
        self.ack_threads_count = ack_message_thread_count
        self.ack_threads = []

        self.reconfigure_signal = Event()
        self.reconfigure_interval_seconds = reconfigure_interval_seconds
        self.reconfigure_thread = None

        # whether to start the consumer thread. Only set to false in unit test
        self.start_consumer_thread = True

    def _do_not_start_consumer_thread(self):
        self.start_consumer_thread = False

    def _reconfigure(self):
        self.logger.info('consumer reconfiguration started')

        hosts = util.execute_frontend(
            self.tchannel, self.deployment_str, {}, self.timeout_seconds, 'readConsumerGroupHosts',
            cherami.ReadConsumerGroupHostsRequest(
                destinationPath=self.path,
                consumerGroupName=self.consumer_group_name
            ))

        host_connections = map(lambda h: util.get_connection_key(h), hosts.hostAddresses) if hosts.hostAddresses is not None else []
        host_connection_set = set(host_connections)
        existing_connection_set = set(self.consumer_threads.keys())
        missing_connection_set = host_connection_set - existing_connection_set
        extra_connection_set = existing_connection_set - host_connection_set

        # clean up
        for extra_conn in extra_connection_set:
            self.logger.info('cleaning up connection %s', extra_conn)
            self.consumer_threads[extra_conn].stop()
            del self.consumer_threads[extra_conn]

        # start up
        for missing_conn in missing_connection_set:
            self.logger.info('creating new connection %s', missing_conn)
            consumer_thread = ConsumerThread(tchannel=self.tchannel,
                                             headers=self.headers,
                                             logger=self.logger,
                                             msg_queue=self.msg_queue,
                                             hostport=missing_conn,
                                             path=self.path,
                                             consumer_group_name=self.consumer_group_name,
                                             timeout_seconds=self.timeout_seconds,
                                             msg_batch_size=self.msg_batch_size
                                             )
            self.consumer_threads[missing_conn] = consumer_thread
            if self.start_consumer_thread:
                consumer_thread.start()

        self.logger.info('consumer reconfiguration succeeded')

    def _start_ack_threads(self):
        for i in range(0, self.ack_threads_count):
            ack_thread = AckThread(tchannel=self.tchannel,
                                   headers=self.headers,
                                   logger=self.logger,
                                   ack_queue=self.ack_queue,
                                   timeout_seconds=self.timeout_seconds)
            ack_thread.start()
            self.ack_threads.append(ack_thread)

    # open the consumer. If succeed, we can start to consume messages
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

            self._start_ack_threads()

            self.logger.info('consumer opened')
        except Exception as e:
            self.logger.exception('Failed to open consumer: %s', e)
            self.close()
            raise e

    # close the consumer
    def close(self):
        if self.reconfigure_thread:
            self.reconfigure_thread.stop()

        for worker in self.consumer_threads.itervalues():
            worker.stop()

        for ack_thread in self.ack_threads:
            ack_thread.stop()

    # Receive messages from cherami. This returns an array of tuple. First value of the tuple is a delivery_token,
    # which can be used to ack or nack the message. The second value of the tuple is the actual message, which is a
    # cherami.ConsumerMessage(in cherami.thrift) object
    def receive(self, num_msgs):
        start_time = time.time()
        timeout_stats = 'cherami_client_python.{}.receive.timeout'.format(self.tchannel.name)
        duration_stats = 'cherami_client_python.{}.receive.duration'.format(self.tchannel.name)

        msgs = []
        end_time = time.time()+self.timeout_seconds
        while len(msgs) < num_msgs:
            seconds_remaining = end_time-time.time()
            if seconds_remaining <= 0:
                stats.count(timeout_stats, 1)
                stats.timing(duration_stats, util.time_diff_in_ms(start_time, time.time()))
                return msgs
            try:
                msgs.append(self.msg_queue.get(block=True, timeout=seconds_remaining))
                self.msg_queue.task_done()

                util.stats_count(self.tchannel.name, 'consumer_msg_queue.dequeue', None, 1)
            except Empty:
                pass
        stats.timing(duration_stats, util.time_diff_in_ms(start_time, time.time()))
        return msgs

    # verify checksum of the message received from cherami
    # return true if the data matches checksum. Otherwise return false
    # Consumer needs to perform this verification and decide what to do based on returned result
    def verify_checksum(self, consumer_message):
        if consumer_message.payload and consumer_message.payload.data:
            if consumer_message.payload.crc32IEEEDataChecksum:
                return util.calc_crc(consumer_message.payload.data, cherami.ChecksumOption.CRC32IEEE) == consumer_message.payload.crc32IEEEDataChecksum
            if consumer_message.payload.md5DataChecksum:
                return util.calc_crc(consumer_message.payload.data, cherami.ChecksumOption.MD5) == consumer_message.payload.md5DataChecksum
        return True

    # Ack can be used by application to Ack a message so it is not delivered to
    # any other consumer
    def ack(self, delivery_token):
        return self._respond(is_ack=True, delivery_token=delivery_token)

    def ack_async(self, delivery_token, callback):
        return self._respond_async(is_ack=True, delivery_token=delivery_token, callback=callback)

    # Nack can be used by application to Nack a message so it can be delivered to
    # another consumer immediately without waiting for the timeout to expire
    def nack(self, delivery_token):
        return self._respond(is_ack=False, delivery_token=delivery_token)

    def nack_async(self, delivery_token, callback):
        return self._respond_async(is_ack=False, delivery_token=delivery_token, callback=callback)

    def _respond(self, is_ack, delivery_token):
        if not delivery_token:
            return

        done_signal = Event()
        result = []

        def callback(ack_result):
            result.append(ack_result)
            done_signal.set()

        self._respond_async(is_ack, delivery_token, callback)

        done = done_signal.wait(self.timeout_seconds)
        if not done or not result:
            self.logger.info({
                'msg': 'ack failure',
                'delivery token': delivery_token,
                'error msg': 'timed out'
            })
            return False
        else:
            if result[0].call_success:
                return True
            else:
                self.logger.info({
                    'msg': 'ack failure',
                    'delivery token': result[0].delivery_token,
                    'error msg': result[0].error_msg
                })
                return False

    def _respond_async(self, is_ack, delivery_token, callback):
        if delivery_token is None or callback is None:
            return

        try:
            self.ack_queue.put((is_ack, delivery_token, callback), block=True, timeout=self.timeout_seconds)

            hostport = util.get_hostport_from_delivery_token(delivery_token)
            util.stats_count(self.tchannel.name, 'consumer_ack_queue.enqueue', hostport, 1)
        except Full:
            callback(AckMessageResult(call_success=False,
                                      is_ack=True,
                                      delivery_token=delivery_token,
                                      error_msg='ack message buffer is full'))
