#!/bin/bash

ssh localhost -p222
cd ~/mini_app/avito_add_image
source venv/bin/activate
python3 manage.py $@
