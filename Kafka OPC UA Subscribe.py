from opcua import Client
import time
import json
import numpy
from opcua import ua
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

while True:

        # khi khong the ket noi voi Kafka thi thong bao loi va ket noi lai
        try:
                # Nhap kafka URL vao cho nay
                client = KafkaClient(hosts='128.199.124.231:9092')
                # nhap kafka topic vao cho nay
                topic = client.topics['Dong03']
                consumer = topic.get_simple_consumer(
                    auto_offset_reset=OffsetType.LATEST,
                    reset_offset_on_start=True)
                print('kafka client connected')
        except:
                print('Khong the ket noi den kafka message broker vui long kiem tra lai ket noi')



        url = "opc.tcp://192.168.20.100:4840"

        client = Client(url)
       # client.connect()


        print("OPC UA Client connected")

        #tao mot dict co ten la data de luu chuoi string nhan duoc tu Kafka
        data = dict()
        try:
                for message in consumer:
                        client.connect()
                        # Get node OPC UA Server
                        # Datablock PLC S7-1500 so thuc REAL

                        real0 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[0]")
                        # data3 = client.get_node("ns=3;s=\"opcua_db\".\"data3\"")
                        # opc_test = real0.get_value()
                        # print(opc_test)
                        # ns = 3 ; s="opc_ua"."REAL"[0]
                        real1 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[1]")
                        real2 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[2]")
                        real3 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[3]")
                        real4 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[4]")

                        # Datablock PLC S7-1500 so nguyen INT

                        int0 = client.get_node("ns=3;s=\"opcua_db\".\"INT\"[0]")
                        int1 = client.get_node("ns=3;s=\"opcua_db\".\"INT\"[1]")
                        int2 = client.get_node("ns=3;s=\"opcua_db\".\"INT\"[2]")
                        int3 = client.get_node("ns=3;s=\"opcua_db\".\"INT\"[3]")
                        int4 = client.get_node("ns=3;s=\"opcua_db\".\"INT\"[4]")

                        # Datablock PLC S7-1500 Boolean

                        Bool0 = client.get_node("ns=3;s=\"opcua_db\".\"BOOL\"[0]")
                        Bool1 = client.get_node("ns=3;s=\"opcua_db\".\"BOOL\"[1]")
                        Bool2 = client.get_node("ns=3;s=\"opcua_db\".\"BOOL\"[2]")
                        Bool3 = client.get_node("ns=3;s=\"opcua_db\".\"BOOL\"[3]")
                        Bool4 = client.get_node("ns=3;s=\"opcua_db\".\"BOOL\"[4]")

                        # PLC S7-1500 Global Memory tag REAL

                        MReal0 = client.get_node("ns=3;s=\"REAL0\"")
                        MReal1 = client.get_node("ns=3;s=\"REAL1\"")
                        MReal2 = client.get_node("ns=3;s=\"REAL2\"")
                        MReal3 = client.get_node("ns=3;s=\"REAL3\"")
                        MReal4 = client.get_node("ns=3;s=\"REAL4\"")

                        # PLC S7-1500 Global Memory tag INT

                        MInt0 = client.get_node("ns=3;s=\"INT0\"")
                        MInt0 = client.get_node("ns=3;s=\"INT1\"")
                        MInt0 = client.get_node("ns=3;s=\"INT2\"")
                        MInt0 = client.get_node("ns=3;s=\"INT3\"")
                        MInt0 = client.get_node("ns=3;s=\"INT4\"")

                        # PLC S7-1500 Global Memoty tag Boolean

                        MBool0 = client.get_node("ns=3;s=\"BOOL0\"")
                        MBool1 = client.get_node("ns=3;s=\"BOOL1\"")
                        MBool2 = client.get_node("ns=3;s=\"BOOL2\"")
                        MBool3 = client.get_node("ns=3;s=\"BOOL3\"")
                        MBool4 = client.get_node("ns=3;s=\"BOOL4\"")

                        #Lay chuoi json tu Kafka va luu vao bien data
                        data = message.value
                        print(data)

                        #split json string
                        pythonObj = json.loads(data)
                        try:
                        #luu python object vao node ids
                                MBOOL0_write = pythonObj["BOOL0"]
                        except:
                                print("")



                        try:
                        #luu python object vao node ids
                                MBOOL1_write = pythonObj["BOOL1"]
                        except:
                                print("")

                        #chuyen doi kieu du lieu python 64bit sang 32bit voi PLC S7-1500

                        MBool0.set_data_value(ua.DataValue(ua.Variant(MBOOL0_write, ua.VariantType.Boolean)))

                        MBool1.set_data_value(ua.DataValue(ua.Variant(MBOOL1_write, ua.VariantType.Boolean)))
                        client.disconnect()

                        # data3.set_data_value(ua.DataValue(ua.Variant(data3_write, ua.VariantType.Float)))
                        # data4.set_data_value(ua.DataValue(ua.Variant(data4_write, ua.VariantType.Float)))
                        #print(data)

        except:
                print('Vui long kiem tra lai ket noi')
                print('******************************************************************************')
                client.disconnect()
