#!/usr/bin/env bash

python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e .[dev]
python -m pip install -r gevent/requirements.txt
