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
import sys
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time

# ====================================================
# MQTT Settings
MQTT_Broker = "broker.hivemq.com"
MQTT_Port = 1883
Keep_Alive_Interval = 45
# ====================================================

def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")



# ====================================================
# FAKE SENSOR
class Sensor:
    ID = 0

    def __init__(self, name, deltatime):
        self._id = Sensor.ID
        Sensor.ID += 1
        self.id = str(name)
        self.geo_cords = near_location(50.059624, 19.916025, 50)
        self.date_n_time = None
        self.temperature = None
        self.humidity = None
        self.PM10 = None
        self.data = None
        self.deltatime = deltatime

        # MQTT Settings
        self.mqttc = None
        self.MQTT_Topic = f"cloud2020/sensors/{self.id}"
        self.AWSClientName = "AWSPython"
        self.AWSPort = 8883
        self.endpoint = "a2uqa59mml9h3u-ats.iot.us-east-1.amazonaws.com"
        self.basePathToCerts = r"C:\Users\szymo\OneDrive\Pulpit\SemestrIII\ChmuryObliczeniowe"
        self.rootCAPath = self.basePathToCerts + r"\rsa.txt"
        self.privateKeyPath = self.basePathToCerts + r"\7d6e952451-private.pem.key"
        self.certificatePath = self.basePathToCerts + r"\7d6e952451-certificate.pem.crt"

    def update_data(self):
        temp = dict()
        temp['id'] = self._id
        temp['sensorID'] = self.id
        temp['cords'] = self.geo_cords
        temp['datetime'] = self.date_n_time
        temp['temperature'] = self.temperature
        temp['humidity'] = self.humidity
        temp['PM10'] = self.PM10
        self.data = temp

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
        self.data = None

    def publish_To_Topic(self, topic, message):
        self.mqttc.publish(topic, message, 1)
        print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))

    def create_json(self):
        print(f'Publishing fake data...')
        print(self.data)
        return json.dumps(self.data)

    def connect_sensor(self):
        self.mqttc = AWSIoTMQTTClient(self.AWSClientName)
        self.mqttc.configureEndpoint(self.endpoint, self.AWSPort)
        self.mqttc.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)

        # AWSIoTMQTTClient connection configuration
        self.mqttc.configureAutoReconnectBackoffTime(1, 32, 20)
        self.mqttc.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.mqttc.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.mqttc.configureConnectDisconnectTimeout(10)  # 10 sec
        self.mqttc.configureMQTTOperationTimeout(5)  # 5 sec

        self.mqttc.connect()
        print('Connected')

        self.mqttc.subscribe(self.MQTT_Topic, 1, customCallback)

        time.sleep(2)

    def publish_Fake_Sensor_Values_to_MQTT(self):
        threading.Timer(self.deltatime, self.publish_Fake_Sensor_Values_to_MQTT).start()
        self.generate_data()
        self.publish_To_Topic(self.MQTT_Topic, self.create_json())
        self.reset_data_buffer()


def main():
    # sensorID = sys.argv[1]
    s1 = Sensor("s1", 1)
    s1.connect_sensor()
    s1.publish_Fake_Sensor_Values_to_MQTT()
    s2 = Sensor("s2", 2)
    s2.connect_sensor()
    s2.publish_Fake_Sensor_Values_to_MQTT()


if __name__ == "__main__":
    main()

# ====================================================
