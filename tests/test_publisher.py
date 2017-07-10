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
import threading
import time
from clay import config

from cherami_client.lib import cherami, cherami_input, util
from cherami_client.client import Client


class TestPublisher(unittest.TestCase):

    def setUp(self):
        self.test_path = '/test/path'
        self.test_msg = 'test_msg'
        self.test_msg_id = 'test_msg_id'
        self.test_receipt = 'test_ext_uuid:xxx'
        self.test_err_msg = 'test_err_msg'
        self.logger = config.get_logger('test')

        self.test_crc32 = util.calc_crc(self.test_msg, cherami.ChecksumOption.CRC32IEEE)
        self.test_md5 = util.calc_crc(self.test_msg, cherami.ChecksumOption.MD5)

        self.publisher_options = mock.Mock(body=cherami.ReadPublisherOptionsResult(
            hostProtocols=[cherami.HostProtocol(
                protocol=cherami.Protocol.TCHANNEL,
                hostAddresses=map(lambda x: cherami.HostAddress(host=str(x), port=x), range(10)))]
        ))

        self.publisher_options_crc32 = mock.Mock(body=cherami.ReadPublisherOptionsResult(
            hostProtocols=[cherami.HostProtocol(
                protocol=cherami.Protocol.TCHANNEL,
                hostAddresses=map(lambda x: cherami.HostAddress(host=str(x), port=x), range(10)))],
            checksumOption=cherami.ChecksumOption.CRC32IEEE
        ))

        self.publisher_options_md5 = mock.Mock(body=cherami.ReadPublisherOptionsResult(
            hostProtocols=[cherami.HostProtocol(
                protocol=cherami.Protocol.TCHANNEL,
                hostAddresses=map(lambda x: cherami.HostAddress(host=str(x), port=x), range(10)))],
            checksumOption=cherami.ChecksumOption.MD5
        ))

        self.publisher_options_diff = mock.Mock(body=cherami.ReadDestinationHostsResult(
            hostProtocols=[cherami.HostProtocol(
                protocol=cherami.Protocol.TCHANNEL,
                hostAddresses=map(lambda x: cherami.HostAddress(host=str(x), port=x), range(7, 15)))]
        ))

        self.send_ack_success = mock.Mock(body=cherami_input.PutMessageBatchResult(
            successMessages=[cherami_input.PutMessageAck(
                id=self.test_msg_id,
                status=cherami.Status.OK,
                receipt=self.test_receipt,
            )]
        ))
        self.send_ack_failed = mock.Mock(body=cherami_input.PutMessageBatchResult(
            failedMessages=[cherami_input.PutMessageAck(
                id=self.test_msg_id,
                status=cherami.Status.FAILED,
                message=self.test_err_msg,
            )]
        ))

        self.mock_call = mock.Mock()
        self.mock_tchannel = mock.Mock()
        self.mock_tchannel.thrift.return_value = self.mock_call

    def test_publisher_create(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)

        publisher = client.create_publisher(self.test_path)
        self.assertEquals(0, len(publisher.workers))
        self.assertIsNone(publisher.reconfigure_thread)

        publisher.open()
        publisher.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BFrontend::readPublisherOptions', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.getPublisherOptionsRequest.path)
        self.assertEquals(10, len(publisher.workers))
        self.assertTrue(publisher.reconfigure_thread.is_alive())

    def test_publisher_publish(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.return_value = self.send_ack_success
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BIn::putMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(1, len(args[0].call_args.request.messages))
        self.assertEquals(self.test_msg_id, args[0].call_args.request.messages[0].id)
        self.assertEquals(self.test_msg, args[0].call_args.request.messages[0].data)
        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.OK, ack.status)
        self.assertEquals(self.test_receipt, ack.receipt)

    def test_publisher_publish_with_context(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.return_value = self.send_ack_success
        context = {'mycontextkey': 'mycontextvalue'}
        ack = publisher.publish(self.test_msg_id, self.test_msg, context)
        publisher.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BIn::putMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(1, len(args[0].call_args.request.messages))
        self.assertEquals(self.test_msg_id, args[0].call_args.request.messages[0].id)
        self.assertEquals(self.test_msg, args[0].call_args.request.messages[0].data)
        self.assertEquals(context, args[0].call_args.request.messages[0].userContext)
        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.OK, ack.status)
        self.assertEquals(self.test_receipt, ack.receipt)

    def test_publisher_publish_failed(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.return_value = self.send_ack_failed
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.FAILED, ack.status)
        self.assertEquals(self.test_err_msg, ack.message)

    def test_publisher_open_exception(self):
        self.mock_call.result.side_effect = Exception(self.test_err_msg)
        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        self.assertRaises(Exception, publisher.open)
        self.assertTrue(publisher.reconfigure_thread is None)

    def test_publisher_publish_exception(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.side_effect = Exception(self.test_err_msg)
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.FAILED, ack.status)
        self.assertTrue(self.test_err_msg in ack.message)

    def test_publisher_publish_timeout(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger, timeout_seconds=1)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        def side_effect():
            time.sleep(3)
            return self.send_ack_success

        self.mock_call.result.side_effect = side_effect
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.TIMEDOUT, ack.status)

    def test_publisher_publish_async(self):
        self.mock_call.result.return_value = self.publisher_options
        done_signal = threading.Event()
        acks = []

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        def callback(ack):
            acks.append(ack)
            done_signal.set()

        self.mock_call.result.return_value = self.send_ack_success
        publisher.publish_async(self.test_msg_id, self.test_msg, callback)
        done_signal.wait(5)
        publisher.close()

        self.assertEquals(1, len(acks))
        self.assertEquals(self.test_msg_id, acks[0].id)
        self.assertEquals(cherami.Status.OK, acks[0].status)
        self.assertEquals(self.test_receipt, acks[0].receipt)

    def test_publisher_publish_async_invalid_callback(self):
        self.mock_call.result.return_value = self.publisher_options

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        # nothing happens if no callback provided (no exception)
        publisher.publish_async(self.test_msg_id, self.test_msg, None)
        publisher.close()

    def test_publisher_publish_crc32(self):
        self.mock_call.result.return_value = self.publisher_options_crc32

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.return_value = self.send_ack_success
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BIn::putMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(1, len(args[0].call_args.request.messages))
        self.assertEquals(self.test_msg_id, args[0].call_args.request.messages[0].id)
        self.assertEquals(self.test_msg, args[0].call_args.request.messages[0].data)
        self.assertEquals(self.test_crc32, args[0].call_args.request.messages[0].crc32IEEEDataChecksum)
        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.OK, ack.status)
        self.assertEquals(self.test_receipt, ack.receipt)

    def test_publisher_publish_md5(self):
        self.mock_call.result.return_value = self.publisher_options_md5

        client = Client(self.mock_tchannel, self.logger)
        publisher = client.create_publisher(self.test_path)
        publisher.open()

        self.mock_call.result.return_value = self.send_ack_success
        ack = publisher.publish(self.test_msg_id, self.test_msg)
        publisher.close()

        args, kwargs = self.mock_tchannel.thrift.call_args
        self.assertEquals('BIn::putMessageBatch', args[0].endpoint)
        self.assertEquals(self.test_path, args[0].call_args.request.destinationPath)
        self.assertEquals(1, len(args[0].call_args.request.messages))
        self.assertEquals(self.test_msg_id, args[0].call_args.request.messages[0].id)
        self.assertEquals(self.test_msg, args[0].call_args.request.messages[0].data)
        self.assertEquals(self.test_md5, args[0].call_args.request.messages[0].md5DataChecksum)
        self.assertEquals(self.test_msg_id, ack.id)
        self.assertEquals(cherami.Status.OK, ack.status)
        self.assertEquals(self.test_receipt, ack.receipt)

    def test_crc32(self):
        s = 'aaa'
        self.assertEquals(util.calc_crc(s, cherami.ChecksumOption.CRC32IEEE), 4027020077)

    # this is a timer based test, disable for now since it's time sensitive
    # def test_publisher_reconfigure(self):
    #     self.mock_call.result.side_effect = [self.input_hosts, self.input_hosts_diff]
    #
    #     client = NewClient(self.mock_tchannel, None)
    #     publisher = client.create_publisher(self.test_path)
    #     publisher.open()
    #     self.assertEquals(10, len(publisher.workers))
    #
    #     time.sleep(15)
    #     self.assertEquals(8, len(publisher.workers))
