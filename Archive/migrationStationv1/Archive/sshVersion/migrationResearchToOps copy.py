import psycopg2
import paramiko

#Config
OPERATIONS_DB_CONFIG = {
    "dbname": "adrc",
    "user": "adrc-admin",
    "password": "admin-secret",
    "host": "adrc-trac.ucsd.edu",
    "port": "5432"
}

SSH_CONFIG = {
    "hostname": "adrc-trac.ucsd.edu",
    "username": "adrc-admin",
    "key_filename": "/Users/savannahhargrave/.ssh/id_adrc_rsa"  
}

def insert_data_into_operations(db_config):

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**SSH_CONFIG)

    transport = ssh.get_transport()
    local_bind_address = ("127.0.0.1", 5432)
    remote_bind_address = ("127.0.0.1", 5432)
    tunnel = transport.open_channel("direct-tcpip", remote_bind_address, local_bind_address)


    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM adrc.combo_salutation;")

    rows = cursor.fetchall()

    for row in rows:
        print(row)


insert_data_into_operations(OPERATIONS_DB_CONFIG)