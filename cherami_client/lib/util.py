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

import os
import pwd
import time

# crc libs
import zlib
import hashlib

from cherami_client.lib import cherami, cherami_output, cherami_input, cherami_frontend
from clay import stats


# helper to execute thrift call
def execute_frontend(tchannel, deployment_str, headers, timeout, method_name, request):
    frontend_module = cherami_frontend.load_frontend(deployment_str)
    method = getattr(frontend_module.BFrontend, method_name)
    if not callable(method):
        raise Exception("Not a valid callable method: " + method_name)

    start_time = time.time()
    try:
        stats_count(tchannel.name, '{}.calls'.format(method_name), None, 1)

        result = tchannel.thrift(method(request), headers=headers, timeout=timeout).result().body

        stats_count(tchannel.name, '{}.success'.format(method_name), None, 1)
        stats_timing(tchannel.name, '{}.duration.success'.format(method_name), start_time)

        return result
    except Exception:
        stats_count(tchannel.name, '{}.exception'.format(method_name), None, 1)
        stats_timing(tchannel.name, '{}.duration.exception'.format(method_name), start_time)
        raise


def execute_input_host(tchannel, headers, hostport, timeout, method_name, request):
    method = getattr(cherami_input.BIn, method_name)
    if not callable(method):
        raise Exception("Not a valid callable method: " + method_name)

    start_time = time.time()
    try:
        stats_count(tchannel.name, '{}.calls'.format(method_name), hostport, 1)

        result = tchannel.thrift(method(request), headers=headers, timeout=timeout, hostport=hostport).result().body

        stats_count(tchannel.name, '{}.success'.format(method_name), hostport, 1)
        stats_timing(tchannel.name, '{}.duration.success'.format(method_name), start_time)

        return result
    except Exception:
        stats_count(tchannel.name, '{}.exception'.format(method_name), hostport, 1)
        stats_timing(tchannel.name, '{}.duration.exception'.format(method_name), start_time)
        raise


def execute_output_host(tchannel, headers, hostport, timeout, method_name, request):
    method = getattr(cherami_output.BOut, method_name)
    if not callable(method):
        raise Exception("Not a valid callable method: " + method_name)

    start_time = time.time()
    try:
        stats_count(tchannel.name, '{}.calls'.format(method_name), hostport, 1)

        result = tchannel.thrift(method(request), headers=headers, timeout=timeout, hostport=hostport).result().body

        stats_count(tchannel.name, '{}.success'.format(method_name), hostport, 1)
        stats_timing(tchannel.name, '{}.duration.success'.format(method_name), start_time)

        return result
    except Exception:
        stats_count(tchannel.name, '{}.exception'.format(method_name), hostport, 1)
        stats_timing(tchannel.name, '{}.duration.exception'.format(method_name), start_time)
        raise


def get_connection_key(host):
    return "{0}:{1}".format(host.host, host.port)


def create_failed_message_ack(id, message):
    return cherami.PutMessageAck(
        id=id,
        status=cherami.Status.FAILED,
        message=message,
    )


def create_timeout_message_ack(id):
    return cherami.PutMessageAck(
        id=id,
        status=cherami.Status.TIMEDOUT,
        message='timeout',
    )


def create_delivery_token(ack_id, hostport):
    return (ack_id, hostport)


def get_ack_id_from_delivery_token(delivery_token):
    return delivery_token[0]


def get_hostport_from_delivery_token(delivery_token):
    return delivery_token[1]


def stats_count(client_name, stats_name, hostport, count):
    overall_stats = 'cherami_client_python.{}.{}'.format(client_name, stats_name)
    stats.count(overall_stats, count)

    if hostport:
        hostport_stats = 'cherami_client_python.{}.{}.{}'.format(client_name, hostport.replace('.','_').replace(':','_'), stats_name)
        stats.count(hostport_stats, count)


def stats_timing(client_name, stats_name, start_time):
    stat_name = 'cherami_client_python.{}.{}'.format(client_name, stats_name)
    stats.timing(stat_name, time_diff_in_ms(start_time, time.time()))


def time_diff_in_ms(t1, t2):
    """Calculate the difference between two timestamps generated by time.time().

    Returned value will be a float rounded to 2 digits after point, representing
    number of milliseconds between the two timestamps.

    :type t1: float
    :type t2: float
    :rtype: float
    """
    return round((t2 - t1) * 1000.0, 2)


def calc_crc(data, crc_type):
    if crc_type == cherami.ChecksumOption.CRC32IEEE:
        # Before python 3.0, the zlib.crc32() returns crc with range [-2**31, 2**31-1], which is incompatible with python 3.0 and GoLang implementation
        # So we need to mask the return value with 0xffffffff. More on https://docs.python.org/2/library/zlib.html
        return zlib.crc32(data) & 0xffffffff
    if crc_type == cherami.ChecksumOption.MD5:
        return hashlib.md5(data).digest()
    return None


def get_username():
    """Gets the username of the user who owns the running process.

    Inspired by https://hg.python.org/cpython/file/3.5/Lib/getpass.py#l155.
    """

    for environ_key in ('LOGNAME', 'USER', 'LNAME', 'USERNAME'):
        username = os.environ.get(environ_key)
        if username:
            return username

    user_id = os.getuid()
    return pwd.getpwuid(user_id)[0]
