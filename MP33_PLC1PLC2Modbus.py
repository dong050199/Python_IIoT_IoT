import pymodbus3
from pandas.tseries import offsets
from pymodbus3.client.sync import ModbusTcpClient
from pymodbus3.constants import Endian
from pymodbus3.payload import BinaryPayloadDecoder
from pymodbus3.payload import BinaryPayloadBuilder
from pymodbus3.client.sync import ModbusTcpClient

import snap7.client as c
from snap7.util import *
from snap7.types import *

import time
import pandas as pd
from openpyxl import load_workbook
from pandas import DataFrame
import xlwt
from xlwt import Workbook
from datetime import datetime
import xlrd
import os
from ctypes import *
import struct
import binascii


def ReadMemory(plc, byte, bit, datatype):
    result = plc.read_area(areas['MK'], 0, byte, datatype)
    if datatype == S7WLBit:
        return get_bool(result, 0, bit)
    elif datatype == S7WLByte or datatype == S7WLWord:
        return get_int(result, 0)
    elif datatype == S7WLReal:
        return get_real(result, 0)
    elif datatype == S7WLDWord:
        return get_dword(result, 0)
    else:
        return None


def WriteMemory(plc, byte, bit, datatype, value):
    result = plc.read_area(areas['MK'], 0, byte, datatype)
    if datatype == S7WLBit:
        set_bool(result, 0, bit, value)
    elif datatype == S7WLByte or datatype == S7WLWord:
        set_int(result, 0, value)
    elif datatype == S7WLReal:
        set_real(result, 0, value)
    elif datatype == S7WLDWord:
        set_dword(result, 0, value)
    plc.write_area(areas["MK"], 0, byte, result)


class DBObject:
    pass


def DBRead(plc, db_num, length, dbitems):
    data = plc.read_area(areas['DB'], db_num, 0, length)
    obj = DBObject()
    for item in dbitems:
        value = None
        offset = int(item['bytebit'].split('.')[0])

        if item['datatype'] == 'Real':
            value = get_real(data, offset)

        if item['datatype'] == 'Bool':
            bit = int(item['bytebit'].split('.')[1])
            value = get_bool(data, offset, bit)

        if item['datatype'] == 'Int':
            value = get_int(data, offset)

        if item['datatype'] == 'String':
            value = get_string(data, offset)

        obj.__setattr__(item['name'], value)

    return obj


def get_db_size(array, bytekey, datatypekey):
    seq, length = [x[bytekey] for x in array], [x[datatypekey] for x in array]
    idx = seq.index(max(seq))
    lastByte = int(max(seq).split('.')[0]) + (offsets[length[idx]])
    return lastByte


def convert(s):
    i = int(s, 16)  # convert from hex to a Python int
    cp = pointer(c_int(i))  # make this into a c integer
    fp = cast(cp, POINTER(c_float))  # cast the int pointer to a float pointer
    return fp.contents.value  # dereference the pointer, get the float


def float_to_hex(f):
    return hex(struct.unpack('<I', struct.pack('<f', f))[0])


if __name__ == "__main__":
    #modbus = ModbusTcpClient('127.0.0.1')
    while True:
        plc1 = c.Client()
        plc3 = c.Client()
        tries = 1
        stop_tries = 2

        while tries < stop_tries and not plc1.get_connected():
            try:

                print('--------->> DANG KET NOI <<----------')
                plc1.connect("192.168.10.51", 0, 1)
                plc3.connect("192.168.10.53", 0, 1)
                time.sleep(1)

                COD_X = 15.642
                TSS_X = 6.321
                FLOW_IN = round(ReadMemory(plc1, 360, 0, S7WLReal), 2)

                value_hex_FLI = float_to_hex(FLOW_IN)
                value_hex_FLI[2:5]
                int_FLI = int(value_hex_FLI[2:6], 16)
                #modbus.write_registers(17, int_FLI)

                value_hex_COD = float_to_hex(COD_X)
                value_hex_COD[2:5]
                int_COD = int(value_hex_COD[2:6], 16)
                #modbus.write_registers(28, int_COD)

                value_hex_TSS = float_to_hex(TSS_X)
                value_hex_TSS[2:5]
                int_TSS = int(value_hex_TSS[2:6], 16)
                #modbus.write_registers(32, int_TSS)

                #AMONI1 = modbus.read_holding_registers(13, 1).registers[0]
                #AMONI2 = modbus.read_holding_registers(14, 1).registers[0]
                #AMONI = round(convert("0x" + hex(AMONI1)[2:6] + hex(AMONI2)[2:6]), 2)

                #COD1 = modbus.read_holding_registers(1, 1).registers[0]
                #COD2 = modbus.read_holding_registers(2, 1).registers[0]
                #COD = round(convert("0x" + hex(COD1)[2:6] + hex(COD2)[2:6]), 2)

                #TEMP1 = modbus.read_holding_registers(15, 1).registers[0]
                #TEMP2 = modbus.read_holding_registers(14, 1).registers[0]
                #TEMP = round(convert("0x" + hex(TEMP1)[2:6] + hex(TEMP2)[2:6]), 2)

                #TSS1 = modbus.read_holding_registers(4, 1).registers[0]
                #TSS2 = modbus.read_holding_registers(6, 1).registers[0]
                #TSS = round(convert("0x" + hex(TSS1)[2:6] + hex(TSS2)[2:6]), 2)

                WriteMemory(plc1, 1206, 0, S7WLReal, AMONI)
                WriteMemory(plc1, 1210, 0, S7WLReal, TEMP)
                WriteMemory(plc3, 340, 0, S7WLReal, COD)
                WriteMemory(plc3, 304, 0, S7WLReal, TSS)

                now = datetime.now()
                current_time = now.strftime("%d/%m/%Y %H:%M:%S")
                print(current_time, AMONI, TEMP, FLOW_IN, COD, TSS)
                time.sleep(3)
            except Exception as e:
                logger.error("------->> MAT KET NOI ADAM PLC <<------->>>>{}".format(e))
                time.sleep(1)

                if tries == (stop_tries - 1):
                    print('------->> VUI LONG KIEM TRA DUONG TRUYEN <<-------')

            tries += 1














