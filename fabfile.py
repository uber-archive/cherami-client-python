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
import sys

from fabric.api import task, local
from fabric.colors import green, red, magenta
from fabric.context_managers import prefix, settings
from functools import wraps

# Path to directory holding virtualenvs.
WORKON_HOME = os.environ.get('WORKON_HOME', os.path.expanduser('~/.virtualenvs'))
# Paths to try when looking for env/bin/activate.
ACTIVATE_PATHS = [
    os.path.join(WORKON_HOME, 'cherami-client-python', 'bin', 'activate'),
]

DEFAULT_ENV = 'development'


def setenv(env='development'):
    """Set clay environment"""
    os.environ['CLAY_CONFIG'] = './config/%s.yaml' % env


def run_in_virtualenv(func):
    """Make sure the task is run in virtualenv (decorator)."""

    @wraps(func)
    def wrapper(*args, **kwargs):

        if os.environ.get('VIRTUAL_ENV'):
            # Already in virtualenv, no need to wrap the function.
            return func(*args, **kwargs)

        # Not in a virtualenv. Search for activate scripts.
        for activate in ACTIVATE_PATHS:
            print activate
            if os.path.isfile(activate):
                with prefix('source %s' % activate):
                    return func(*args, **kwargs)

        raise EnvironmentError('Unable to find a python virtualenv!')

    return wrapper


@task
def clean():
    """Remove all .pyc files."""
    print green('Clean up .pyc files')
    local("find . -name '*.py[co]' -exec rm -f '{}' ';'")


@task
def lint():
    """Check for lints"""
    print green('Checking for lints')
    local("flake8 `find . -name '*.py' -not -path '*env/*' -not -path '*receiptservice_client/*'"
          " -not -path '*cherami-client-python/thrift/*'` --ignore=E501,W503,E702,E712")


@task
def bootstrap(env=DEFAULT_ENV):
    """Bootstrap the environment."""
    local("mkdir -p logs")
    print green("\nInstalling requirements")
    local("pip install -r requirements-test.txt")
    local("pip install -r requirements.txt")

@task
def test(args="", cov_report="term-missing", junit_xml=None, arc_cover=False):
    """Run the test suite."""

    os.environ['CLAY_CONFIG'] = './config/test.yaml'
    cmd = ("py.test tests -rs --tb short %s "
           "--cov cherami-client-python --cov-report %s") % (args, cov_report)

    if junit_xml:
        cmd = "%s --junit-xml %s" % (cmd, junit_xml)

    with settings(warn_only=True, quiet=True):
        success = local(cmd).succeeded

    # This is needed until arc diff doesn't need cov report in coverage/
    if arc_cover:
        local("mv coverage.xml coverage/cobertura-coverage.xml")

    if success:
        print(green("Tests finished running with success."))
    else:
        print(red("Test finished running with errors."))
        sys.exit(1)

@task
def shell(env=DEFAULT_ENV):
    """Run the shell in the environment."""
    os.environ['CLAY_CONFIG'] = './config/%s.yaml' % env
    # local("ipython --ipython-dir ./config/")  # useful if ipython is installed
    local("python")