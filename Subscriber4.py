# python 3.11

import random

from datetime import datetime
from paho.mqtt import client as mqtt_client


import json
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation



broker = 'broker.emqx.io'
port = 1883
topic = "python/mqttGK"
# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

arr_time = []
arr_value = []
arr_value1 = []
arr_value2 = []


def subscribe(client: mqtt_client):


    def on_message(client, userdata, msg):
        dados = json.loads(msg.payload.decode())

        arr_time.append(dados["time"])
        arr_value1.append(dados["cpu_percent"])
        arr_value2.append(dados["mousex"]/10)
        print(arr_time)
        print(arr_value1)
        print(arr_value2)

        #text = msg.payload.decode("utf-8")
#        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#        update(plt.gcf())

    client.subscribe(topic)
    client.on_message = on_message




def column(matrix, i):
    return [row[i] for row in matrix]


# updates the data and graph
def update(frame):
    global arr_time
    global arr_value1
    global arr_value2
    global plt

    # creating a new graph or updating the graph

    plt.cla()
    plt.plot(arr_time, arr_value1)
    plt.plot(arr_time, arr_value2)

    # creating a new graph or updating the graph
    id_ini = len(arr_time)-10
    if id_ini <= 0: 
        id_ini = 0
#    plt.xlim(arr_time[id_ini], arr_time[len(arr_time)-1])
#    plt.ylim(0, 100)


from threading import Thread

def threaded_function():
    global plt
    plt.tight_layout()
    anim = FuncAnimation(plt.gcf(), update, interval = 10)
    plt.show()


client = connect_mqtt()
subscribe(client)


thread = Thread(target = threaded_function)
thread.start()

client.loop_forever()

thread.join()



def run():
    print("running!")


if __name__ == '__main__':
    run()





