#!/bin/bash

pathToQuery=$1
outTablePath=$2

if [ -f "$pathToQuery" ]; then
    scp -i ~/.ssh/id_adrc_rsa -P 9221 $pathToQuery adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/queryScripts
else
    echo "No query found. Please double check your path:" $pathToQuery >&2
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

    
    export PGPASSWORD=\$POSTGRES_SUPER_PASSWORD
    (echo "password: '\$POSTGRES_SUPER_PASSWORD'")

    if [[ -z \$outTablePath ]]; then 
        psql -h localhost -U adrc-admin -d adrc -f /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/queryScripts/$(basename "$pathToQuery")
    else
        psql -h localhost -U adrc-admin -d adrc --no-align --field-separator=',' \
         -f /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/queryScripts/$(basename "$pathToQuery") \
         > /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables/$(basename "$outTablePath")
    fi

EOF

if [[ -n $outTablePath ]]; then
    scp -i ~/.ssh/id_adrc_rsa -P 9221 adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables/$(basename "$outTablePath") /Users/savannahhargrave/adrc/TRAC/migrationStation/"$outTablePath"
fi