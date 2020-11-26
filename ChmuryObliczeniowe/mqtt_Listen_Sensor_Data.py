# ------------------------------------------
# --- Author: Pradeep Singh
# --- Changes: Szymon Trela
# --- Date: 2020
# --- Version: 1.1
# --- Python Ver: 3.8
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------

import paho.mqtt.client as mqtt
from store_Sensor_Data_to_DB import sensor_data_handler

# MQTT Settings
MQTT_Broker = "broker.hivemq.com"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "STR/#"


# Subscribe to all Sensors at Base Topic
def on_connect(mqttc, userdata, flags, rc):
    mqttc.subscribe(MQTT_Topic, 0)


# Save Data into DB Table
def on_message(mqttc, obj, msg):
    # This is the Master Call for saving MQTT Data into DB
    # For details of "sensor_Data_Handler" function please refer "sensor_data_to_db.py"
    print(f'MQTT Data Received...\n'
          f'MQTT Topic: {msg.topic}\n'
          f'Data: {msg.payload}\n')
    sensor_data_handler(msg.topic, msg.payload)


def on_subscribe(mqttc, obj, mid, granted_qos):
    pass


mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
