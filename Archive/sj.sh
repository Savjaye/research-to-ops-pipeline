#!/bin/bash

# if .env file present ... 
if [ -f /home/adrc-admin/adrc/deliverables/.env ]; then 
    . /home/adrc-admin/adrc/deliverables/.env # source its contents 
else 
    echo 'Missing .env file' >&2
    exit 2
fi

    
export PGPASSWORD=$POSTGRES_SUPER_PASSWORD
(echo "password: '$POSTGRES_SUPER_PASSWORD'")

psql -h localhost -U adrc-admin -d adrc 