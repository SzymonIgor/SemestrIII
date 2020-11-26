# ------------------------------------------
# --- Author: Pradeep Singh
# --- Changes: Szymon Trela
# --- Date: 2020
# --- Version: 1.1
# --- Python Ver: 3.8
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
from near_location import near_location

# ====================================================
# MQTT Settings
MQTT_Broker = "broker.hivemq.com"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "STR/D212020"
# ====================================================


def on_connect(mqttc, userdata, flags, rc):
    if rc != 0:
        print('Unable to connect to MQTT Broker...\n')
    else:
        print(f'Connected with MQTT Broker: {MQTT_Broker}\n')


def on_publish(mqttc, userdata, mid):
    print("AA")
    pass


def on_disconnect(mqttc, userdata, rc):
    if rc != 0:
        pass


mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print(f'Published: {message}\n'
          f'on MQTT Topic: {topic}\n')


# ====================================================
# FAKE SENSOR
class Sensor:
    ID = 0

    def __init__(self, name, delta_time):
        self.delta_time = delta_time
        self._id = Sensor.ID
        Sensor.ID += 1
        self.id = str(name)
        self.geo_cords = near_location(50.059624, 19.916025, 50)
        self.date_n_time = None
        self.temperature = None
        self.humidity = None
        self.PM10 = None
        self.data = {'_id':         self._id,
                     'sensor_id':   self.id,
                     'cords':       self.geo_cords,
                     'datetime':    [],
                     'temperature': [],
                     'humidity':    [],
                     'PM10':        []}

    def update_data(self):
        self.data['datetime'].append(self.date_n_time)
        self.data['temperature'].append(self.temperature)
        self.data['humidity'].append(self.humidity)
        self.data['PM10'].append(self.PM10)

        self.date_n_time = None
        self.temperature = None
        self.humidity = None
        self.PM10 = None

    def generate_data(self):
        self.date_n_time = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        self.temperature = f'{20 + (random.random() - .5) * 20:.2f}'  # 20 +-10
        self.humidity = f'{50 + (random.random() - .5) * 80:.2f}'  # 50 +-40
        self.PM10 = f'{abs(25 + (random.random() - .5) * 100):.2f}'  # 25 +-50

        self.update_data()

    def reset_data_buffer(self):
        self.data['datetime'] = []
        self.data['temperature'] = []
        self.data['humidity'] = []
        self.data['PM10'] = []

    def collect_data(self):
        print(f'Publishing fake data...')
        print(self.data)
        publish_To_Topic(MQTT_Topic, json.dumps(self.data))
        self.reset_data_buffer()


    def start(self):
        threading.Timer(self.delta_time, self.start, ).start()
        self.generate_data()
        if len(self.data['datetime']) >= 10:
            self.collect_data()


s1 = Sensor('s1', 3)
s1.start()
s2 = Sensor('s2', 1)
s2.start()

# ====================================================
