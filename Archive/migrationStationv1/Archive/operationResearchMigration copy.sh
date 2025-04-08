#!/bin/bash

query=$(cat ./insertCustomer.sql)
ssh -i ~/.ssh/id_adrc_rsa -p 9221 adrc-admin@adrc-trac.ucsd.edu << EOF 

    # navigate to where the database and environment info lives
    cd adrc/deliverables
    
    # if .env file present ... 
    if [ -f .env ]; then 
        . /home/adrc-admin/adrc/deliverables/.env # source its contents 
    else 
        echo 'Missing .env file' >&2
        exit 1
    fi

    # ? why is did the 'POSTGRES_SUPER_PASSWORD' change? I have to hard code it for now which is not the most secure...
    export PGPASSWORD='admin-secret'

    echo "$query" | psql -h localhost -U adrc-admin -d adrc --no-align --field-separator=',' > /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/secureCopyStorage/customers.csv
EOF

scp -i ~/.ssh/id_adrc_rsa -P 9221 adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/secureCopyStorage/customers.csv /Users/savannahhargrave/adrc/TRAC/localQueries/

