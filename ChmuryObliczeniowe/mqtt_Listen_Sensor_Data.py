# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------

import paho.mqtt.client as mqtt
from store_Sensor_Data_to_DB import sensor_data_handler
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time


class SensorListener:
    def __init__(self):
        self.data_list = []
        # MQTT Settings
        self.mqttc = None
        self.MQTT_Topic_Get = "cloud2020/sensors/#"
        self.MQTT_Topic_Group = "cloud2020/DataGroup"
        self.AWSClientName = "SensorListener"
        self.AWSPort = 8883
        self.endpoint = "a2uqa59mml9h3u-ats.iot.us-east-1.amazonaws.com"
        self.basePathToCerts = r"C:\Users\szymo\OneDrive\Pulpit\SemestrIII\ChmuryObliczeniowe"
        self.rootCAPath = self.basePathToCerts + r"\rootca.txt"
        self.privateKeyPath = self.basePathToCerts + r"\7d6e952451-private.pem.key"
        self.certificatePath = self.basePathToCerts + r"\7d6e952451-certificate.pem.crt"

    def connect_to_broker(self):
        self.mqttc = AWSIoTMQTTClient(self.AWSClientName)
        self.mqttc.configureEndpoint(self.endpoint, self.AWSPort)
        self.mqttc.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)

        # AWSIoTMQTTClient connection configuration
        self.mqttc.configureAutoReconnectBackoffTime(1, 32, 20)
        self.mqttc.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.mqttc.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.mqttc.configureConnectDisconnectTimeout(10)  # 10 sec
        self.mqttc.configureMQTTOperationTimeout(20)  # 20 sec

        self.mqttc.connect()
        print('Connected')

        self.mqttc.subscribe(self.MQTT_Topic_Get, 1, self.customCallback)
        self.mqttc.subscribe(self.MQTT_Topic_Group, 1, self.groupCallback)

    def publish_To_Topic(self, topic, message):
        self.mqttc.publish(topic, message, 1)
        print("Published")
        # print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))

    # Custom MQTT message callback
    def customCallback(self, client, userdata, message):
        print("Collected")
        # print(message.payload)
        self.data_list.append(message.payload.decode("utf-8"))

    def sender(self):
        while True:
            time.sleep(0.001)
            if len(self.data_list) >= 10:
                temp = self.data_list
                self.data_list = []
                for payload in temp:
                    self.publish_To_Topic(self.MQTT_Topic_Group, str(payload))
                print("Published")

    def groupCallback(self, client, userdata, message):
        print("Message sent")


def main():
    sL = SensorListener()
    sL.connect_to_broker()
    sL.sender()


if __name__ == "__main__":
    main()

