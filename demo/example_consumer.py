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

from __future__ import absolute_import, print_function

import time

from demo.example_client import client


destination = '/test/dest'
consumer_group = '/test/cg'

while True:
    try:
        consumer = client.create_consumer(destination, consumer_group)
        consumer.open()
        print('Consumer created.')
        break
    except Exception as e:
        print('Failed to create a consumer: %s', e)
        time.sleep(2)

try:
    results = consumer.receive(num_msgs=2)
    for res in results:
        delivery_token = res[0]
        msg = res[1]
        try:
            print(msg.payload.data)
            consumer.ack(delivery_token)
        except Exception as e:
            consumer.nack(delivery_token)
            print('Failed to process a message: %s', e)
            pass
except Exception as e:
    consumer.close()
    print('Failed to receive messages: ', e)

consumer.close()
