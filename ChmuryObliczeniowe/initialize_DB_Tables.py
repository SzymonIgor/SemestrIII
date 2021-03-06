# ------------------------------------------
# --- Author: Pradeep Singh
# --- Changes: Szymon Trela
# --- Date: 2020
# --- Version: 1.1
# --- Python Ver: 3.8
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------

import sqlite3

# SQLite DB Name
DB_Name = "IoT.db"

# SQLite DB Table Schema
TableSchema = """
drop table if exists STR_Data ;
create table STR_Data (
  id integer primary key autoincrement,
  _ID integer,
  SensorID text,
  Latitude real,
  Longitude real,
  Date_n_Time text,
  Temperature text,
  Humidity text,
  PM10 text
);
"""

# Connect or Create DB File
conn = sqlite3.connect(DB_Name)
curs = conn.cursor()

# Create Tables
sqlite3.complete_statement(TableSchema)
curs.executescript(TableSchema)

# Close DB
curs.close()
conn.close()
