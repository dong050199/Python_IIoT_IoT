import s71200_direct
from time import sleep
import snap7
from snap7.util import *
import struct
from snap7.exceptions import Snap7Exception

client = snap7.client.Client()


import paho.mqtt.client as paho
#mqtt library
import os
import json
import time
from datetime import datetime


ACCESS_TOKEN="KaaYU61IJ3PecHiWzSAe"
#Token of your device
broker="thingsboard.cloud"
#host name
port=1883
#data listening port

def on_publish(client,userdata,result):
    #create function for callback
    #print("data published to thingsboard \n")
    pass
client1= paho.Client("control1")
#create client object
client1.on_publish = on_publish
#assign function to callback
client1.username_pw_set(ACCESS_TOKEN)
#access token from thingsboard device
client1.connect(broker,port,keepalive=6)
#establish connection
a= 100

while True:
        #print(client.get_connected())
        if client.get_connected() == False:
            try:
                #print('not connected')
                plc = s71200_direct.S71200("192.168.0.2")
                #client.connect()
                #print('not connected')
                #time.sleep(0.2)
            except Snap7Exception as e:
                continue
                #time.sleep(10)
        else:
            print('connected')
    # make json string withoud
        payload = "{"
        payload += "\"PLC_TAG2\":%s" % plc.getMem('QX0.0')
        payload += "}"
        ret = client1.publish("v1/devices/me/telemetry", payload)
        # topic-v1/devices/me/telemetry
        print(payload);
        client.disconnect()
        time.sleep(1)