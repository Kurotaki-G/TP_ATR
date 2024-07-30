# python 3.11

#inatividade ok!!
#grafico único ok!!
#gráfico separado ok
#db alarme ok

#iniciar a aplicação via comando de linha:
#"python3 nomedoarquivo.py periodoMsgIni periodoVal1 periodoVal2"

import sys

import random
import time

from datetime import date
from datetime import datetime

from paho.mqtt import client as mqtt_client

import uuid

from threading import Thread


import json
import psutil
import pyautogui




uuid = uuid.uuid4()

print('Your UUID is: ' + str(uuid))

now = datetime.now()
uuid = now.strftime("%H:%M:%S")


id_da_maquina = "MAQ_"+str(uuid)

sensors = [
        {
            "sensor_id": "cpu_percent",
            "data_type": "FLOAT",
            "data_interval": 2
        },
        {
            "sensor_id": "sensors_battery",
            "data_type": "FLOAT",
            "data_interval": 1
        }
]




broker = 'broker.emqx.io'
port = 1883
topic = "/sensors/" + id_da_maquina +"/"

# Generate a Client ID with the publish prefix.
#client_id = f'publish-{random.randint(0, 1000)}'

# Generate a Client ID with the publish prefix.
client_id = 'publish-'+id_da_maquina

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

def sensor_loop(client, sensor_id, data_interval, ):
    msg_count = 1

    while True:

        if sensor_id != "cpu_percent": 
            time.sleep(data_interval)


        td = date.today()
        today = td.strftime("%Y-%m-%d")
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")

        current_time = datetime.now().isoformat()

        current_time = current_time.split(".")[0]

        if sensor_id == "cpu_percent":
            value = psutil.cpu_percent(interval=data_interval)
        elif sensor_id == "sensors_battery":
#            value = psutil.sensors_temperatures()[0]
            value = pyautogui.position()[0]



#        >> for x in range(3):
#        ...     psutil.cpu_percent(interval=1)
#        ...
#        4.0
#        5.9
#        3.8
#        >>>
#        >>> for x in range(3):
#        ...     psutil.cpu_percent(interval=1, percpu=True)
#        ...
#        [4.0, 6.9, 3.7, 9.2]
#        [7.0, 8.5, 2.4, 2.1]
#        [1.2, 9.0, 9.9, 7.2]
#        >>> psutil.sensors_temperatures()
#        {'acpitz': [shwtemp(label='', current=47.0, high=103.0, critical=103.0)],
#         'asus': [shwtemp(label='', current=47.0, high=None, critical=None)],
#         'coretemp': [shwtemp(label='Physical id 0', current=52.0, high=100.0, critical=100.0),
#                      shwtemp(label='Core 0', current=45.0, high=100.0, critical=100.0)]}
#        >>>
#        >>> psutil.sensors_fans()
#        {'asus': [sfan(label='cpu_fan', current=3200)]}
#        >>>
#        >>> psutil.sensors_battery()
#        sbattery(percent=93, secsleft=16628, power_plugged=False)

        
        dataTosend = {
          "time_stamp": current_time,         
          "value": value,
        }

        # convert into JSON:
        msg = json.dumps(dataTosend)

        topic = "/sensors/"+id_da_maquina+"/"+sensor_id

        result = client.publish(topic, msg)

        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")

      


def iniMsg_loop(client, ini_msg_interval, ):
    while True:

        array_tosend = {
            "machine_id": id_da_maquina,
            "sensors": sensors
        }

        time.sleep(ini_msg_interval)

        # convert into JSON:
        msg = json.dumps(array_tosend)


        topic = "/sensor_monitors"

        result = client.publish(topic, msg)

        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")

def run():

    

    client = connect_mqtt()
    client.loop_start()

    sensors[0]["data_interval"] = int(sys.argv[2])
    sensors[1]["data_interval"] = int(sys.argv[3])

    ini_msg_interval = int(sys.argv[1])

    thread_sensor1 = Thread(target = sensor_loop, args = (client, sensors[0]["sensor_id"],sensors[0]["data_interval"], ))
    thread_sensor1.start()
    thread_sensor2 = Thread(target = sensor_loop, args = (client, sensors[1]["sensor_id"],sensors[1]["data_interval"], ))
    thread_sensor2.start()

    thread_iniMsg = Thread(target = iniMsg_loop, args = (client, ini_msg_interval ))
    thread_iniMsg.start()

    client.loop_stop()

    thread_sensor1.join()
    thread_sensor2.join()



if __name__ == '__main__':
    run()
