

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