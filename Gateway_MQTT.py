#library for read and write s7-1200 siemens PLC
import s71200_direct
from time import sleep
import snap7
from snap7.util import *
import struct
#library for publis and subscribe MQTT
import paho.mqtt.client as paho
#mqtt library
import os
import json
import time
from datetime import datetime

#change your PLC IP in here if rack and slot =! 0,1 you can change it in s71200_direct.py
plc = s71200_direct.S71200("192.168.0.2")



ACCESS_TOKEN="KaaYU61IJ3PecHiWzSAe"
#Token of your device
broker="thingsboard.cloud"
#host name
port=1883
#data listening port

def on_publish(client,userdata,result):
    #create function for callback
    print("data published to thingsboard \n")
    pass
client1= paho.Client("control1")
#create client object
client1.on_publish = on_publish
#assign function to callback
client1.username_pw_set(ACCESS_TOKEN)
#access token from thingsboard device
client1.connect(broker,port,keepalive=60)
#establish connection
a= 100

while True:
#write tag name to PLC_TAGX and address for getMem to read PLC and make MQTT to transfer to thingsboard
#make json string withoud
    payload="{"
    payload += "\"PLC_TAG1\":%s," % plc.getMem("FREAL612")
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s," % plc.getMem('QX0.0')
    payload += "\"PLC_TAG2\":%s" % plc.getMem('QX0.0')
    payload+="}"
    ret= client1.publish("v1/devices/me/telemetry",payload)
    #topic-v1/devices/me/telemetry
    print(payload);
    time.sleep(1)
#sent every 60s
