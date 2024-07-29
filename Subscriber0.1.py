# python 3.11

import random

import sqlite3

from datetime import datetime
from paho.mqtt import client as mqtt_client


import json
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

from threading import Thread



broker = 'broker.emqx.io'
port = 1883
topic = "python/mqttGK"
# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'



## BANCO DE DADOS ##
# Conectando ao banco de dados (ou criando um novo, se não existir)
conn = sqlite3.connect('meu_banco.db')

# Criando um cursor para executar comandos SQL
cursor = conn.cursor()

# Criando uma tabela
cursor.execute('''
CREATE TABLE IF NOT EXISTS my_table (
    id INTEGER PRIMARY KEY,
    data TEXT
)
''')
conn.commit()
## fim -- BANCO DE DADOS ##



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

def on_message(client, userdata, msg):
    dados = json.loads(msg.payload.decode())
    if msg.topic == "/sensor_monitors":
        on_iniMsg(dados, msg.topic)
    else:
        if split(msg.topic, "/")[1] = "sensors": #lembrar de corrigir no publisher!!
            on_sensorMsg(dados, msg.topic)

# DISPOSITIVOS 
array_maquinas: ["nome1", "nome2"]


def on_iniMsg(dados, topic):
    
    #recebeu o iniMsg de uma máquina.
    #confere se ela já está na lista de array_maquinas:
    if id_da_maquina in array_maquinas:
        
    else:
        
        
        # Criando um cursor para executar comandos SQL
        cursor = conn.cursor()
        
        # Criando uma tabela
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS "machine-id.alarms.alarm-type" (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        ''')

        # Criando uma tabela
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS "machine-id.sensor1" (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS "machine-id.sensor2" (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
        ''')
        client.subscribe("/sensors/"+id_da_maquina+"/sensor1")
        client.subscribe("/sensors/"+id_da_maquina+"/sensor2")

        thread confere_atv1 = Thread(confere_atividade, arg = (topic,data_interval,) )
        thread confere_atv2 = Thread(confere_atividade, arg = (topic,data_interval,) )

                            
def confere_atividade(topic, data_interval):
    last_timestamp = time.time() #???
    
    id_da_maquina = split(topic,"/")[1]
    id_sensor = split(topic,"/")[2]
    nome_tabela = id_da_maquina + "." + id_sensor
    
    while(true):
        time.wait(10*data_interval); #delay???
        #acessa a tabela de nome: "id_da_maquina.id_sensor"
        #pega o ultimo item do banco.
        #atualiza o lastimestamp de acordo como ultimo enviado e comara
        if time.time() - last_timestamp > 10*data_interval:
            break
            
    client.unsbscribe(topic)
    #thread morre
        

def on_sensorMsg(dados, topic):

        id_da_maquina = split(topic,"/")[1]
        id_sensor = split(topic,"/")[2]
        nome_tabela = id_da_maquina + "." + id_sensor
    
        #acessa a tabela de nome: "id_da_maquina.id_sensor"
        #escreve na última linha

        arr_time["id_da_maquina.sensor1"].append(dados["time"])
        arr_time["id_da_maquina.sensor2"].append(dados["cpu_percent"])
        arr_value2.append(dados["mousex"]/10)
        print(arr_time)
        print(arr_value1)
        print(arr_value2)

        # convert into JSON:
        msg = json.dumps(dados)

        #insere na tabela os valores recebidos
        cursor.execute('INSERT INTO '+ nome_tabela +' (data) VALUES (?)', (msg,))
        conn.commit()

        #text = msg.payload.decode("utf-8")
#        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#        update(plt.gcf())


def subscribe_inicial(client: mqtt_client):

    

    client.subscribe("/sensor_monitors")
    client.on_message = on_message




def column(matrix, i):
    return [row[i] for row in matrix]


# updates the data and graph
def update(frame):
    global arr_time
    global arr_value1
    global arr_value2
    global plt

    #acessa o banco de dados
    #carrega os valores de tempo e de valores 1 e 2.

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




def threaded_function():
    global plt
    plt.tight_layout()
    anim = FuncAnimation(plt.gcf(), update, interval = 10000)
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





