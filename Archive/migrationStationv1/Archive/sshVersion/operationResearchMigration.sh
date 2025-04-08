#!/bin/bash

pathToQuery=$1
query=$(cat "$pathToQuery")
outTableName=$2

if [ -f "/Users/savannahhargrave/adrc/TRAC/localQueries/$pathToQuery" ]; then
    scp -i ~/.ssh/id_adrc_rsa -P 9221  /Users/savannahhargrave/adrc/TRAC/localQueries/$pathToQuery adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/queryScripts
else
    echo "No query found. Please doudle check your path:" $pathToQuery >&2
    exit 1
fi

ssh -i ~/.ssh/id_adrc_rsa -p 9221 adrc-admin@adrc-trac.ucsd.edu << EOF 

    # navigate to where the database and environment info lives
    cd adrc/deliverables
    
    # if .env file present ... 
    if [ -f .env ]; then 
        . /home/adrc-admin/adrc/deliverables/.env # source its contents 
    else 
        echo 'Missing .env file' >&2
        exit 2
    fi

    # ? why is did the 'POSTGRES_SUPER_PASSWORD' change? I have to hard code it for now which is not the most secure...
    export PGPASSWORD='admin-secret'

    psql -h localhost -U adrc-admin -d adrc --no-align --field-separator=',' > /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables/$outTableName <<SQL
    $query
    SQL

EOF

scp -i ~/.ssh/id_adrc_rsa -P 9221 adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables/$outTableName /Users/savannahhargrave/adrc/TRAC/localQueries/tables

