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

import os

from tchannel import thrift

thrift_file = '../idl/cherami.thrift'
default_service_name = 'cherami-frontendhost'

frontend_modules = {'': thrift.load(
    path=os.path.join(
        os.path.dirname(__file__),
        thrift_file,
    ),
    service=default_service_name,
)}


def load_frontend(env=''):
    if env is None or env.lower().startswith('prod') or env.lower().startswith('dev'):
        env = ''

    if env in frontend_modules:
        return frontend_modules[env]

    service_name = default_service_name
    if env:
        service_name += '_'
        service_name += env

    frontend_modules[env] = thrift.load(
        path=os.path.join(
            os.path.dirname(__file__),
            thrift_file,
        ),
        service=service_name,
    )

    return frontend_modules[env]
