#!/bin/bash
export LC_ALL=C
apt-get install libpng-dev libfreetype6-dev python-tk -y
cat requirements.txt | xargs -n 1 pip install
python setup.py develop
