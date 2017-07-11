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

import unittest
import mock
from clay import config

from cherami_client.lib import cherami, cherami_output
from cherami_client.client import Client


class TestConsumer(unittest.TestCase):

    def setUp(self):
        self.test_path = '/test/path'
        self.test_cg = 'test/cg'
        self.test_msg = cherami_output.PutMessage(data='msg')
        self.test_ack_id = 'test_ack_id'
        self.logger = config.get_logger('test')
        self.test_err_msg = 'test_err_msg'
        self.test_delivery_token = (self.test_ack_id, '0:0')

        self.output_hosts = mock.Mock(body=cherami.ReadConsumerGroupHostsResult(
            hostAddresses=map(lambda x: cherami.HostAddress(host=str(x), port=x), range(10))
        ))

        self.received_msgs = mock.Mock(body=cherami_output.ReceiveMessageBatchResult(
            messages=[cherami_output.ConsumerMessage(
                ackId=self.test_ack_id,
                payload=self.test_msg
            )]
        ))

        self.no_msg = mock.Mock(body=cherami_output.ReceiveMessageBatchResult(
            messages=[]
        ))

        self.ack_ok_response = mock.Mock(body=None)

        self.mock_call = mock.Mock()
        self.mock_tchannel = mock.Mock()
        self.mock_tchannel.thrift.return_value = self.mock_call

    def test_consumer_create(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)

        consumer = client.create_consumer(self.test_path, self.test_cg)
        self.assertEquals(0, len(consumer.consumer_threads))
        self.assertEquals(0, len(consumer.ack_threads))
        self.assertIsNone(consumer.reconfigure_thread)

        consumer._do_not_start_consumer_thread()
        consumer.open()
        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BFrontend::readConsumerGroupHosts', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.getHostsRequest.destinationPath)
        self.assertEquals(self.test_cg, args[0].call_args.getHostsRequest.consumerGroupName)
        self.assertEquals(10, len(consumer.consumer_threads))
        self.assertEquals(consumer.ack_threads_count, len(consumer.ack_threads))
        self.assertTrue(consumer.reconfigure_thread.is_alive())

    def test_consumer_consume(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        consumer._do_not_start_consumer_thread()
        consumer.open()

        self.mock_call.result.return_value = self.received_msgs
        consumer.consumer_threads['0:0'].start()
        msgs = consumer.receive(1)
        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BOut::receiveMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(self.test_cg, args[0].call_args.request.consumerGroupName)
        self.assertEquals(1, len(msgs))
        self.assertEquals(self.test_msg, msgs[0][1].payload)
        self.assertEquals(self.test_ack_id, msgs[0][1].ackId)

    def test_consumer_open_exception(self):
        self.mock_call.result.side_effect = Exception(self.test_err_msg)
        client = Client(self.mock_tchannel, self.logger)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        self.assertRaises(Exception, consumer.open)
        self.assertTrue(consumer.reconfigure_thread is None)

    def test_consumer_no_message(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        consumer._do_not_start_consumer_thread()
        consumer.open()

        self.mock_call.result.return_value = self.no_msg
        consumer.consumer_threads['0:0'].start()
        msgs = consumer.receive(1)
        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BOut::receiveMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(self.test_cg, args[0].call_args.request.consumerGroupName)
        self.assertEquals(0, len(msgs))

    def test_consumer_exception(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        consumer._do_not_start_consumer_thread()
        consumer.open()

        self.mock_call.result.side_effect = Exception('exception')
        consumer.consumer_threads['0:0'].start()
        msgs = consumer.receive(1)

        # verify the thread is still alive even though an excepton has been thrown by output host
        self.assertTrue(consumer.consumer_threads['0:0'].is_alive())

        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BOut::receiveMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(self.test_cg, args[0].call_args.request.consumerGroupName)
        self.assertEquals(0, len(msgs))

    def test_consumer_ack_ok(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        consumer._do_not_start_consumer_thread()
        consumer.open()

        self.mock_call.result.return_value = self.ack_ok_response
        res = consumer.ack(self.test_delivery_token)
        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BOut::ackMessages', args[0].endpoint)
        self.assertEquals(1, len(args[0].call_args.ackRequest.ackIds))
        self.assertEquals(self.test_delivery_token[0], args[0].call_args.ackRequest.ackIds[0])
        self.assertTrue(res)

    def test_consumer_ack_fail(self):
        self.mock_call.result.return_value = self.output_hosts

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        consumer = client.create_consumer(self.test_path, self.test_cg)
        consumer._do_not_start_consumer_thread()
        consumer.open()

        self.mock_call.result.side_effect = Exception('exception')
        res = consumer.ack(self.test_delivery_token)
        consumer.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BOut::ackMessages', args[0].endpoint)
        self.assertEquals(1, len(args[0].call_args.ackRequest.ackIds))
        self.assertEquals(self.test_delivery_token[0], args[0].call_args.ackRequest.ackIds[0])
        self.assertFalse(res)
