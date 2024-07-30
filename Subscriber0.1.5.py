# python 3.11

#inatividade ok!!
#grafico único ok!!
#gráfico separado ok
#db alarme ok

#obs: o nome da máquina a ser exibida é gerado pelo horário. ex: MAQ_12:34:56
#para visualizar o gráfico da máquina desejada, colocar na chamada:
#python3 Subscriber0.1.5.py MAQ_12:34:56 cpu_percent sensors_battery


import sys
import time

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


# DISPOSITIVOS 
array_maquinas = ["maq1"]

def on_message(client, userdata, msg):
    dados = json.loads(msg.payload.decode())

    if msg.topic == "/sensor_monitors":

        on_initialMsg(dados, client)
    else:
        
        if msg.topic.split("/")[1] == "sensors": #lembrar de corrigir no publisher!!
            on_sensorMsg(dados, msg.topic)


def iso_to_int(time_stamp):

    date = time_stamp.split("T")[0]
    hour = time_stamp.split("T")[1]

    total = float(hour.split(":")[2])
    total += int(hour.split(":")[1]) * 60 #min
    total += int(date.split("-")[0]) * 60 * 60 #hour
    total += int(date.split("-")[2]) * 60 * 60 * 24 #date
    total += (int(date.split("-")[1]) - 1) * 60 * 60 * 24 *30
    total += (int(date.split("-")[0]) - 1970) * 60 * 60 * 24 * 365
    return total 
               
def confere_atividade(topic, data_interval):

    global array_maquinas

    num_inativo = 10
    
    id_da_maquina = topic.split("/")[2]
    id_sensor = topic.split("/")[3]
    nome_tabela = id_da_maquina + "." + id_sensor
    
    # Conectando ao banco de dados (ou criando um novo, se não existir)
    conn = sqlite3.connect('meu_banco.db')

    # Criando um cursor para executar comandos SQL
    cursor_consulta = conn.cursor()

    while(True):
        time.sleep(num_inativo*data_interval); #delay???

        print("Conferindo inatividade... - "+id_da_maquina)

        current_time = datetime.now().isoformat()
        last_timestamp = current_time #provisório, apenas para caso dê erro no SQL ter valor válido

        for row in cursor_consulta.execute("SELECT time_stamp, value FROM '"+id_da_maquina+"."+id_sensor+"' ORDER BY id DESC LIMIT "+str(num_inativo)):
            last_timestamp = row[0]



        if iso_to_int(current_time) - iso_to_int(last_timestamp) > num_inativo*data_interval:
            print("Inatividade de - "+id_da_maquina)
            #insere na tabela o alerta
            cursor_consulta.execute("INSERT INTO '"+ id_da_maquina+".alarms.inactive"+"' (time_stamp, last_received, alarm_status) VALUES (?, ?, ?)", (current_time, last_timestamp, 1))
            conn.commit()
            break
            
#    print("Subscrição cancelada por inatividade - "+id_da_maquina)
    client.unsubscribe(topic)
    print("Subscrição cancelada por inatividade - "+id_da_maquina)
    if id_da_maquina in array_maquinas:
        array_maquinas.remove(id_da_maquina)
    #thread morre
        

def on_initialMsg(dados, client):


    global array_maquinas

    id_da_maquina = dados["machine_id"]

    sensor1 = dados["sensors"][0]["sensor_id"]
    sensor2 = dados["sensors"][1]["sensor_id"]

    data_interval1 = dados["sensors"][0]["data_interval"]
    data_interval2 = dados["sensors"][1]["data_interval"]

    data_type1 = dados["sensors"][0]["data_type"]
    data_type2 = dados["sensors"][1]["data_type"]

    
    topic1 = "/sensors/"+id_da_maquina+"/"+sensor1
    topic2 = "/sensors/"+id_da_maquina+"/"+sensor2

    if id_da_maquina in array_maquinas:
        print("Configuração recebida  - "+id_da_maquina)

    else:

        # Criando um cursor para executar comandos SQL
        cursor = conn.cursor()
        
        # Criando uma tabela
        cursor.execute("CREATE TABLE IF NOT EXISTS '"+id_da_maquina+".alarms.inactive' (id INTEGER PRIMARY KEY, time_stamp TIMESTAMP, last_received TIMESTAMP, alarm_status INTEGER)")
        cursor.execute("CREATE TABLE IF NOT EXISTS '"+id_da_maquina+"."+sensor1+"' (id INTEGER PRIMARY KEY, time_stamp TIMESTAMP, value "+data_type1+")")
        cursor.execute("CREATE TABLE IF NOT EXISTS '"+id_da_maquina+"."+sensor2+"' (id INTEGER PRIMARY KEY, time_stamp TIMESTAMP, value "+data_type2+")")

        client.subscribe(topic1,1)
        client.subscribe(topic2,1)

        confere_atv1 = Thread(target = confere_atividade, args = (topic1, data_interval1, ) )
        confere_atv2 = Thread(target = confere_atividade, args = (topic2, data_interval2, ) ) 
        confere_atv1.start()
        confere_atv2.start()

        print("Subscrições realizadas - "+id_da_maquina)

        array_maquinas.append(id_da_maquina)

def on_sensorMsg(dados, topic):

        #acessa a tabela de nome: "id_da_maquina.id_sensor"
        #escreve na última linha


        id_da_maquina = topic.split("/")[2]
        id_sensor = topic.split("/")[3]
        nome_tabela = id_da_maquina + "." + id_sensor

        print("Dados do sensor recebidos - "+id_da_maquina)

        # convert into JSON:
        msg = json.dumps(dados)

        #insere na tabela os valores recebidos
        cursor.execute("INSERT INTO '"+ nome_tabela +"' (time_stamp, value) VALUES (?, ?)", (dados["time_stamp"], dados["value"]))
        conn.commit()

        #text = msg.payload.decode("utf-8")
#        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#        update(plt.gcf())




sel_maquina = ""
sel_sensor1 = ""
sel_sensor2 = ""



arr_time1 = []
arr_time2 = []
arr_value1 = []
arr_value2 = []


# updates the data and graph
def update(frame):


    global sel_maquina1
    global sel_maquina2
    global sel_sensor1
    global sel_sensor2

    global plt

    qtd_dados = 20
    
    i = 0

    arr_time1.clear()
    arr_time2.clear()
    arr_value1.clear()
    arr_value2.clear()

    ## BANCO DE DADOS ##
    # Conectando ao banco de dados (ou criando um novo, se não existir)
    conn = sqlite3.connect('meu_banco.db')

    # Criando um cursor para executar comandos SQL
    cursor_consulta = conn.cursor()
    

    for row in cursor_consulta.execute("SELECT time_stamp, value FROM '"+sel_maquina1+"."+sel_sensor1+"' ORDER BY id DESC LIMIT "+str(qtd_dados)):
        arr_time1.append(row[0].split("T")[1])
        arr_value1.append(row[1])
    arr_time1.reverse()
    arr_value1.reverse()

    for row in cursor_consulta.execute("SELECT time_stamp, value FROM '"+sel_maquina2+"."+sel_sensor2+"' ORDER BY id DESC LIMIT "+str(qtd_dados)):
        arr_time2.append(row[0].split("T")[1])
        arr_value2.append(row[1])
    arr_time2.reverse()
    arr_value2.reverse()


#    plt.cla()
#    plt.figure(1)
    plt.cla()
    plt.subplot(211)
    plt.cla()
#    plt.figure(2)
    plt.plot(arr_time1, arr_value1)
    plt.subplot(212)
    plt.plot(arr_time2, arr_value2)

    # creating a new graph or updating the graph



plt.tight_layout()

def threaded_function():
    global plt

    plt.subplot(211)
    plt.subplot(212)
#    anim = FuncAnimation(plt.gcf(), update, interval = 1000, save_count = 1000)
    plt.show()


client = connect_mqtt()

client.subscribe("/sensor_monitors",1)

client.on_message = on_message

sel_maquina1 = sys.argv[1]
sel_sensor1 = sys.argv[2]
sel_sensor2 = sys.argv[3]
sel_maquina2 = sel_maquina1

#sel_maquina2 = sys.argv[3]
#sel_sensor2 = sys.argv[4]


thread = Thread(target = threaded_function)
thread.start()

client.loop_forever()

thread.join()



def run():
    print("running!")


if __name__ == '__main__':
    run()




