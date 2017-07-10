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


class ReconfigureThread(threading.Thread):
    def __init__(self, interval_seconds, reconfigure_signal, reconfigure_func, logger):
        threading.Thread.__init__(self)
        self.interval_seconds = interval_seconds
        self.reconfigure_signal = reconfigure_signal
        self.reconfigure_func = reconfigure_func
        self.logger = logger
        self.stop_signal = threading.Event()

    def stop(self):
        self.stop_signal.set()

        # set the reconfigure_signal so that we won't be blocked on
        # waiting for reconfig signal timeout
        self.reconfigure_signal.set()

    def run(self):
        while not self.stop_signal.is_set():
            # trigger on either signal or wait timeout
            self.reconfigure_signal.wait(self.interval_seconds)

            if self.stop_signal.is_set():
                return

            try:
                self.reconfigure_func()
            except Exception:
                self.logger.info('reconfiguration thread {0}, exception {1}'.format(threading.current_thread(), traceback.format_exc()))
                pass

            # done reconfigure, reset signal if needed
            if self.reconfigure_signal.is_set():
                self.reconfigure_signal.clear()
