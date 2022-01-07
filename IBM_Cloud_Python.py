import json
import paho.mqtt.client as mqtt
import time

client = mqtt.Client('d:5yj5uf:python:python')

client.username_pw_set('use-token-auth','PythonMqtt')
client.connect('5yj5uf.messaging.internetofthings.ibmcloud.com', 8883, 60)

payload = {'d':{'temperature':21}}

while True:
    client.publish('iot-2/type/python/id/python/evt/event/fmt/json', json.dumps(payload))
    print(payload);
    time.sleep(1)
    client.loop()