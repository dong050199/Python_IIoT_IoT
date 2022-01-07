from opcua import Client
from pykafka import KafkaClient
import time
import json

while True:
        try:
                try:

                        #Kết nối với topic Dong01 của kafka.
                        kafka_client = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic = kafka_client.topics['Dong01']
                        kafka_producer = kafka_topic.get_sync_producer()

                        #kết nối với topic Dong02 của kafka
                        kafka_client1 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic1 = kafka_client1.topics['Dong02']
                        kafka_producer1 = kafka_topic1.get_sync_producer()
                except:
                        print('ERROR KAFKA Connection')
                try:
                        url = "opc.tcp://192.168.20.100:4840"
                        client = Client(url)
                        client.connect()
                        print("OPC UA Client connected")
                except:
                        print('ERROR OPC UA CONNECTION')

                datablock = dict()
                memory = dict()
                while True:
                        # Get node OPC UA Server
                        # Datablock PLC S7-1500 so thuc REAL

                        real0 = client.get_node("ns=3;s=\"opcua_db\".\"REAL\"[0]")
                        #data3 = client.get_node("ns=3;s=\"opcua_db\".\"data3\"")
                        #opc_test = real0.get_value()
                        #print(opc_test)
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
                        MInt1 = client.get_node("ns=3;s=\"INT1\"")
                        MInt2 = client.get_node("ns=3;s=\"INT2\"")
                        MInt3 = client.get_node("ns=3;s=\"INT3\"")
                        MInt4 = client.get_node("ns=3;s=\"INT4\"")

                                # PLC S7-1500 Global Memoty tag Boolean

                        MBool0 = client.get_node("ns=3;s=\"BOOL0\"")
                        MBool1 = client.get_node("ns=3;s=\"BOOL1\"")
                        MBool2 = client.get_node("ns=3;s=\"BOOL2\"")
                        MBool3 = client.get_node("ns=3;s=\"BOOL3\"")
                        MBool4 = client.get_node("ns=3;s=\"BOOL4\"")



                        #READ STATE STATION

                        Station1_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION1_STATE\"")
                        Station2_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION2_STATE\"")
                        Station3_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION3_STATE\"")
                        Station4_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION4_STATE\"")
                        Station5_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION5_STATE\"")
                        Station6_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION6_STATE\"")
                        Station7_State = client.get_node("ns=3;s=\"Station_MES\".\"STATION7_STATE\"")

                        # Tao mot bien kieu du lieu dict de tao chuoi json

                        datablock["REAL0"] = real0.get_value()
                        datablock["REAL1"] = real1.get_value()
                        datablock["REAL2"] = real2.get_value()
                        datablock["REAL3"] = real3.get_value()
                        datablock["REAL4"] = real4.get_value()

                        # *************************************************

                        datablock["INT0"] = int0.get_value()
                        datablock["INT1"] = int1.get_value()
                        datablock["INT2"] = int2.get_value()
                        datablock["INT3"] = int3.get_value()
                        datablock["INT4"] = int4.get_value()

                        # **********************************************************

                        datablock["BOOL0"] = Bool0.get_value()
                        datablock["BOOL1"] = Bool1.get_value()
                        datablock["BOOL2"] = Bool2.get_value()
                        datablock["BOOL3"] = Bool3.get_value()
                        datablock["BOOL4"] = Bool4.get_value()


                        #************************************************************

                        datablock["STATION1_STATE"] = Station1_State.get_value()
                        datablock["STATION2_STATE"] = Station2_State.get_value()
                        datablock["STATION3_STATE"] = Station3_State.get_value()
                        datablock["STATION4_STATE"] = Station4_State.get_value()
                        datablock["STATION5_STATE"] = Station5_State.get_value()
                        datablock["STATION6_STATE"] = Station6_State.get_value()
                        datablock["STATION7_STATE"] = Station7_State.get_value()



                        # *************************************************

                        memory["MREAL0"] = MReal0.get_value()
                        memory["MREAL1"] = MReal1.get_value()
                        memory["MREAL2"] = MReal2.get_value()
                        memory["MREAL3"] = MReal3.get_value()
                        memory["MREAL4"] = MReal4.get_value()

                        # *************************************************

                        memory["MINT0"] = MInt0.get_value()
                        memory["MINT1"] = MInt1.get_value()
                        memory["MINT2"] = MInt2.get_value()
                        memory["MINT3"] = MInt3.get_value()
                        memory["MINT4"] = MInt4.get_value()

                        # *************************************************

                        memory["MBOOL0"] = MBool0.get_value()
                        memory["MBOOL1"] = MBool1.get_value()
                        memory["MBOOL2"] = MBool2.get_value()
                        memory["MBOOL3"] = MBool3.get_value()
                        memory["MBOOL4"] = MBool4.get_value()

                        datablock_out = json.dumps(datablock)
                        memory_out = json.dumps(memory)
                        print(datablock_out)
                        print(memory_out)

                        try:
                                kafka_producer.produce(str(datablock_out).encode('ascii'))
                                time.sleep(3)
                                kafka_producer1.produce(str(memory_out).encode('ascii'))
                                time.sleep(3)
                                print('Data published')
                                time.sleep(5)
                        except:
                                print('ERROR WHEN PUBLISH DATA PLEASE CHECK CONNECTION')

        except:
                print('Error when connect please check cable or internet')