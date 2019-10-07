#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Clear any cached results
find . -name "*.pyc" -exec rm -f {} \;

echo "Running style checks"
flake8

export PYTHONPATH=plugins

echo "Running unit tests"
pytest

echo "Running coverage tests"
pytest --cov=plugins --cov-report html --cov-fail-under=100
