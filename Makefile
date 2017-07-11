.PHONY: help bootstrap clean lint test coverage docs release install jenkins

help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "release - package and upload a release"
	@echo "install - install the package to the active Python's site-packages"

bootstrap:
	virtualenv --setuptools env
	. env/bin/activate
	pip install --upgrade setuptools
	pip install --upgrade "pip>=7,<8"
	pip install -r requirements.txt
	pip install -r requirements-test.txt

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage
	rm -fr htmlcov/

lint:
	flake8 cherami_client tests

test:
	python setup.py test $(TEST_ARGS)

jenkins: test

coverage: test
	coverage run --source cherami_client setup.py test
	coverage report -m
	coverage html
	open htmlcov/index.html

release: clean
	fullrelease

install: clean
	python setup.py install
