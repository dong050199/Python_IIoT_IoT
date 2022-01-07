from opcua import Client
import time
import json
import numpy
from opcua import ua

url = "opc.tcp://192.168.20.55:4840"
client = Client(url)
client.connect()
print("OPC UA Client connected")



while True:
    try:
        temp = client.get_node("ns=3;s=\"opcua_db\".\"data1\"")
        press = client.get_node("ns=3;s=\"opcua_db\".\"data2\"")
        value = 15.03131312312312312312
        #dv = ua.DataValue(ua.Variant(value,ua.VariantType.Float))
        #temp.set_data_value(dv)
        #temp.set_data_value(ua.DataValue(ua.Variant(<type your value here>,ua.VariantType.Float)))
        temp.set_data_value(ua.DataValue(ua.Variant(value,ua.VariantType.Float)))

        time.sleep(5)
    except Exception as e:
        continue