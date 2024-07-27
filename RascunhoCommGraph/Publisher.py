# python 3.11

# envia os dados de cpu e as posições do mouse

import random
import time

from datetime import date
from datetime import datetime

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
topic = "python/mqttGK"
# Generate a Client ID with the publish prefix.
client_id = f'publish-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'

def connect_mqtt():
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


import json
import psutil
import pyautogui


periodo = 1

def publish(client, periodo):
    msg_count = 1

    while True:
#        time.sleep(0.1)

        td = date.today()
        today = td.strftime("%Y-%m-%d")
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
#        value = random.randint(0, 100)

        cpu_percent = psutil.cpu_percent(interval=periodo)
        value2 = psutil.virtual_memory()[2]
        value2 = psutil.swap_memory()
        value2 = psutil.disk_usage('/')[3]
        value2 = psutil.cpu_freq()[0]
        mousex = pyautogui.position()[0]
        mousey = -pyautogui.position()[1]
        
        dataTosend = {
          "date": today,
          "time": current_time,
          "value": value2,
          "cpu_percent": cpu_percent,
          "mousex": mousex,
          "mousey": mousey
        }
        #msg = f"olaa {msg_count} {today}, {current_time}"

        # convert into JSON:
        msg = json.dumps(dataTosend)

        result = client.publish(topic, msg)


        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


#        msg_count += 1
        if msg_count > 10:
            break




def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client, 2)
    client.loop_stop()



if __name__ == '__main__':
    run()
