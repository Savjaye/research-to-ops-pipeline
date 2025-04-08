#!/bin/bash

adrcIDs=$1
firstHalfQuery='SELECT * FROM adrc.combo_salutation WHERE id IN'
secondHalfQuery=';'


ssh -i ~/.ssh/id_adrc_rsa -p 9221 adrc-admin@adrc-trac.ucsd.edu << EOF 

    # navigate to where the database and environment info lives
    cd adrc/deliverables
    
    # if .env file present ... 
    if [ -f .env ]; then 
        . /home/adrc-admin/adrc/deliverables/.env # source its contents 
    else 
        echo "Missing .env file" >&2
        exit 1
    fi

    export PGPASSWORD=\$POSTGRES_SUPER_PASSWORD
    echo 'SELECT * FROM adrc.combo_salutation' | psql -h localhost -U adrc-admin -d adrc --no-align --field-separator=',' > /home/adrc-admin/adrc/deliverables/sjhScriptsQueries/secureCopyStorage/test.csv
EOF

scp -i ~/.ssh/id_adrc_rsa -P 9221 adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/secureCopyStorage/test.csv /Users/savannahhargrave/adrc/TRAC/localQueries/
