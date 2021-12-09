# Daemon to extract unsynchronized rows from Retina DB

# Path para compilador en Ubuntu
# ! /usr/bin/python3

import json
import pymysql
import pysftp
from datetime import datetime
import os.path


def extract_config():
    with open("configuracion.json") as json_file:
        config_t = json.load(json_file)

    host_t = config_t["host"]
    user_t = config_t["user"]
    passwd_t = config_t["passwd"]
    db_t = config_t["db"]
    last_check_t = config_t["last_check"]
    ip_retina_t = config_t["ip_retina"]

    return host_t, user_t, passwd_t, db_t, last_check_t, ip_retina_t, config_t


def packing_data(host_sql, user_sql, passwd_sql, db_sql, last_check_sql, ip_retina_sql):
    # database connection
    connection = pymysql.connect(host=host_sql, user=user_sql, passwd=passwd_sql, database=db_sql)
    cursor = connection.cursor()

    if last_check_sql == "":
        retrieve = "SELECT * FROM outdated_data_sensores WHERE sincronizado=0"
    else:
        retrieve = f"SELECT * FROM outdated_data_sensores WHERE (fecha_hora > '{last_check_sql}' ) AND sincronizado=0"

    cursor.execute(retrieve)
    raw_data = cursor.fetchall()

    retrieve = "SELECT id, retina_sensores FROM id_sensores"
    cursor.execute(retrieve)
    sensor_id_list = cursor.fetchall()

    # committing the connection then closing it.
    connection.commit()
    connection.close()

    # Converting tuples to list
    raw_data = [list(elem) for elem in raw_data]
    # changing id
    id_dictionary = {str(sensor_id[0]): str(sensor_id[1]) for sensor_id in sensor_id_list}

    # Sorting by sensor and changing sincronizado value then saving in data
    sensor = {}
    fechas = []
    for block in raw_data:
        block[1] = id_dictionary[str(block[1])]
        block[3] = 1
        temp = sensor.get(block[1], [])
        temp.append(block[2])
        sensor[block[1]] = temp
        fechas.append(str(block[0]))

    fechas = list(dict.fromkeys(fechas))

    # Getting the length of each sensor list
    lists_sizes = [len(value) for key, value in sensor.items()]
    lists_sizes.sort()

    # Make all the lists equal in length
    for key, temp in sensor.items():
        temp = temp[:lists_sizes[0]]
        sensor[key] = temp

    # Taking rows and packing the data in blocks
    package_data_t = []
    for size_value in range(lists_sizes[0]):
        temp2 = {}
        dict_bloque = {}
        for block in sensor:
            temp = sensor[block]
            temp2[block] = temp[size_value]
        dict_bloque["ip_retina"] = ip_retina_sql
        dict_bloque["fecha_hora"] = fechas[size_value]
        dict_bloque["sensores"] = temp2
        package_data_t.append(dict_bloque)

    return package_data_t


if __name__ == '__main__':
    host, user, passwd, db, last_check, ip_retina, config = extract_config()
    package_data = packing_data(host, user, passwd, db, last_check, ip_retina)

    # Saving data in JSON
    now = datetime.now()
    saving_name = now.strftime("%m-%d-%Y@%H_%M_%S.json")
    saving_path = 'C:\\Users\\emili\\OneDrive\\Escritorio'
    full_path = os.path.join(saving_path, saving_name)
    with open(full_path, 'w', encoding='utf-8') as f:
        json.dump(package_data, f, ensure_ascii=False, indent=4, default=str)

    # variables to establish connection with SFTP
    sftp_host = "yourserverdomainorip.com"
    sftp_user = "root"
    sftp_passwd = "12345"

    # Mover archivo a servidor SFTP
    with pysftp.Connection(sftp_host, username=sftp_user, password=sftp_passwd) as sftp:
        localFilePath = full_path
        remoteFilePath = f'/var/integraweb-db-backups/{saving_name}'
        sftp.put(localFilePath, remoteFilePath)

    # Actualizar last_check
    last_value = len(package_data) - 1
    last_check = package_data[last_value]
    config["last_check"] = last_check["fecha_hora"]

    with open("configuracion.json", 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4, default=str)
