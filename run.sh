#!/usr/bin/env bash

set -euo pipefail

python main.py 3.11 > py311.json
python main.py 3.10 > py310.json
python main.py 3.9 > py39.json
