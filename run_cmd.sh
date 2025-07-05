#!/bin/bash

ssh localhost -p222
cd ~/bazon_to_avito/avito_add_image
source venv/bin/activate
python3 manage.py $@
