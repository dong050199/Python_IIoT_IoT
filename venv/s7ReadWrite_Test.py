import s71200_direct
from time import sleep
import snap7
from snap7.util import *
import struct
from snap7.snap7exceptions import Snap7Exception

client = snap7.client.Client()
print(client.get_connected())
#print(snap7.exceptions.Snap7Exception())
plc = s71200_direct.S71200("192.168.0.1")
print(snap7.exceptions.Snap7Exception())
print(client.get_connected())

print (plc.getMem('MX0.1'))
# read memory bit M0.1
print (plc.getMem('QX0.0'))
print (plc.getMem("FREAL100"))
print (plc.getMem("MW200"))
print (plc.writeMem("FREAL100",105))
print (plc.writeMem('MX0.1',True))
print (plc.writeMem('QX0.0',True))
print (plc.writeMem("MB200",110))
plc.plc.disconnect()
