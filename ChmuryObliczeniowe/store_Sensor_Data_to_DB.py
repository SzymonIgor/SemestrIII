# ------------------------------------------
# --- Author: Pradeep Singh
# --- Changes: Szymon Trela
# --- Date: 2020
# --- Version: 1.1
# --- Python Ver: 3.8
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import json
import sqlite3

# SQLite DB Name
DB_Name = "IoT.db"

# ===============================================================
# Database Manager Class


class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect(DB_Name)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()

# ===============================================================
# Functions to push Sensor Data into Database


# Function to save Temperature to DB Table
def temp_data_handler(json_data):
    json_dict = json.loads(json_data)
    _id = int(json_dict['_id'])
    sensor_id = json_dict['sensor_id']
    cords = json_dict['cords']
    datetime = json_dict['datetime']
    temperature = json_dict['temperature']
    humidity = json_dict['humidity']
    PM10 = json_dict['PM10']

    #Push into DB Table
    for i, item in enumerate(datetime):
        dbObj = DatabaseManager()
        # dbObj.add_del_update_db_record('insert into STR_Data '
        #                                '(_ID, SensorID, Latitude, Longitude, Date_n_Time, Temperature, Humidity, PM10) '
        #                                'values (?, ?, ?, ?, ?, ?, ?, ?)',
        #                                [_id, sensor_id, 0, 0,
        #                                 datetime, temperature[i], humidity[i], PM10[i]])
        dbObj.add_del_update_db_record('insert into STR_Data '
                                       '(_ID, SensorID, Latitude, Longitude, Date_n_Time, Temperature, Humidity, PM10) '
                                       'values (?, ?, ?, ?, ?, ?, ?, ?)',
                                       [_id, sensor_id, cords['Latitude'], cords['Longitude'],
                                        datetime[i], temperature[i], humidity[i], PM10[i]])
        del dbObj
    print("Inserted Temperature Data into Database.\n")


# Function to save Humidity to DB Table
# ===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_data_handler(topic, json_data):
    temp_data_handler(json_data)

# ===============================================================
