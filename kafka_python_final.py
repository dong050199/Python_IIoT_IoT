from opcua import Client
from pykafka import KafkaClient
import time
import json

while True:
        try:
                try:
                        # connect to topic station1
                        kafka_client = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic = kafka_client.topics['station1']
                        kafka_producer = kafka_topic.get_sync_producer()
                        # connect to topic station2
                        kafka_client1 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic1 = kafka_client1.topics['station2']
                        kafka_producer1 = kafka_topic1.get_sync_producer()
                        # connect to topic station3
                        kafka_client2 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic2 = kafka_client2.topics['station3']
                        kafka_producer2 = kafka_topic2.get_sync_producer()
                        # connect to topic station4
                        kafka_client3 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic3 = kafka_client3.topics['station4']
                        kafka_producer3 = kafka_topic3.get_sync_producer()
                        # connect to topic station5
                        kafka_client4 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic4 = kafka_client4.topics['station5']
                        kafka_producer4 = kafka_topic4.get_sync_producer()
                        # connect to topic station6
                        kafka_client5 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic5 = kafka_client5.topics['station6']
                        kafka_producer5 = kafka_topic5.get_sync_producer()
                        #connect to topic station7
                        kafka_client6 = KafkaClient(hosts='128.199.124.231:9092')
                        kafka_topic6 = kafka_client6.topics['station7']
                        kafka_producer6 = kafka_topic6.get_sync_producer()

                except:
                        print('ERROR KAFKA Connection')
                try:
                        url = "opc.tcp://192.168.20.100:4840"
                        client = Client(url)
                        client.connect()
                        print("OPC UA Client connected")
                except:
                        print('ERROR OPC UA CONNECTION')

                data_station1 = dict()
                data_station2 = dict()
                data_station3 = dict()
                data_station4 = dict()
                data_station5 = dict()
                data_station6 = dict()
                data_station7 = dict()
                while True:
                        # Get node OPC UA Server
                        # Datablock PLC S7-1500 so thuc REAL

                        #real0 = client.get_node("ns=3;s=\"station1\".\"sen1\"")
                        #data3 = client.get_node("ns=3;s=\"opcua_db\".\"data3\"")
                        #opc_test = real0.get_value()
                        #print(opc_test)
                        # ns = 3 ; s="opc_ua"."REAL"[0]

                        s1_sen1 = client.get_node("ns=3;s=\"station1\".\"sen1\"")
                        s1_sen2 = client.get_node("ns=3;s=\"station1\".\"sen2\"")
                        s1_m1 = client.get_node("ns=3;s=\"station1\".\"m1\"")
                        s1_NOP = client.get_node("ns=3;s=\"station1\".\"NOP\"")
                        s1_OEE = client.get_node("ns=3;s=\"station1\".\"OEE\"")
                        s1_STATE = client.get_node("ns=3;s=\"station1\".\"STATE\"")

                        data_station1["STATION1_SEN1"] = s1_sen1.get_value()
                        data_station1["STATION1_SEN2"] = s1_sen2.get_value()
                        data_station1["STATION1_M1"] = s1_m1.get_value()
                        data_station1["NOP"] = s1_NOP.get_value()
                        data_station1["OEE"] = s1_OEE.get_value()
                        data_station1["STATION1_STATE"] = s1_STATE.get_value()
#********************************************************************************
                        s2_sen1 = client.get_node("ns=3;s=\"station2\".\"sen1\"")
                        s2_sen2 = client.get_node("ns=3;s=\"station2\".\"sen2\"")
                        s2_m1 = client.get_node("ns=3;s=\"station2\".\"m1\"")
                        s2_NOP = client.get_node("ns=3;s=\"station2\".\"NOP\"")
                        s2_OEE = client.get_node("ns=3;s=\"station2\".\"OEE\"")
                        s2_STATE = client.get_node("ns=3;s=\"station2\".\"STATE\"")

                        data_station2["STATION2_SEN1"] = s2_sen1.get_value()
                        data_station2["STATION2_SEN2"] = s2_sen2.get_value()
                        data_station2["STATION2_M1"] = s2_m1.get_value()
                        data_station2["NOP"] = s2_NOP.get_value()
                        data_station2["OEE"] = s2_OEE.get_value()
                        data_station2["STATION2_STATE"] = s2_STATE.get_value()
# ********************************************************************************
                        s3_sen1 = client.get_node("ns=3;s=\"station3\".\"sen1\"")
                        s3_sen2 = client.get_node("ns=3;s=\"station3\".\"sen2\"")
                        s3_m1 = client.get_node("ns=3;s=\"station3\".\"m1\"")
                        s3_NOP = client.get_node("ns=3;s=\"station3\".\"NOP\"")
                        s3_OEE = client.get_node("ns=3;s=\"station3\".\"OEE\"")
                        s3_STATE = client.get_node("ns=3;s=\"station3\".\"STATE\"")

                        data_station3["STATION3_SEN1"] = s3_sen1.get_value()
                        data_station3["STATION3_SEN2"] = s3_sen2.get_value()
                        data_station3["STATION3_M1"] = s3_m1.get_value()
                        data_station3["NOP"] = s3_NOP.get_value()
                        data_station3["OEE"] = s3_OEE.get_value()
                        data_station3["STATION3_STATE"] = s3_STATE.get_value()
# ********************************************************************************
                        s4_sen1 = client.get_node("ns=3;s=\"station4\".\"sen1\"")
                        s4_sen2 = client.get_node("ns=3;s=\"station4\".\"sen2\"")
                        s4_sen3 = client.get_node("ns=3;s=\"station4\".\"sen3\"")
                        s4_sen4 = client.get_node("ns=3;s=\"station4\".\"sen4\"")
                        s4_m1 = client.get_node("ns=3;s=\"station4\".\"m1\"")
                        s4_m2 = client.get_node("ns=3;s=\"station4\".\"m2\"")
                        s4_NOP = client.get_node("ns=3;s=\"station4\".\"NOP\"")
                        s4_OEE = client.get_node("ns=3;s=\"station4\".\"OEE\"")
                        s4_STATE = client.get_node("ns=3;s=\"station4\".\"STATE\"")

                        data_station4["STATION4_SEN1"] = s4_sen1.get_value()
                        data_station4["STATION4_SEN2"] = s4_sen2.get_value()
                        data_station4["STATION4_SEN3"] = s4_sen3.get_value()
                        data_station4["STATION4_SEN4"] = s4_sen4.get_value()
                        data_station4["STATION4_M1"] = s4_m1.get_value()
                        data_station4["STATION4_M2"] = s4_m2.get_value()
                        data_station4["NOP"] = s4_NOP.get_value()
                        data_station4["OEE"] = s4_OEE.get_value()
                        data_station4["STATION4_STATE"] = s4_STATE.get_value()
# ********************************************************************************
                        s5_sen1 = client.get_node("ns=3;s=\"station5\".\"sen1\"")
                        s5_sen2 = client.get_node("ns=3;s=\"station5\".\"sen2\"")
                        s5_sen3 = client.get_node("ns=3;s=\"station5\".\"sen3\"")
                        s5_sen4 = client.get_node("ns=3;s=\"station5\".\"sen4\"")
                        s5_sen5 = client.get_node("ns=3;s=\"station5\".\"sen5\"")
                        s5_sen6 = client.get_node("ns=3;s=\"station5\".\"sen6\"")
                        s5_sen7 = client.get_node("ns=3;s=\"station5\".\"sen7\"")
                        s5_m1 = client.get_node("ns=3;s=\"station5\".\"m1\"")
                        s5_m2 = client.get_node("ns=3;s=\"station5\".\"m2\"")
                        s5_NOP = client.get_node("ns=3;s=\"station5\".\"NOP\"")
                        s5_OEE = client.get_node("ns=3;s=\"station5\".\"OEE\"")
                        s5_STATE = client.get_node("ns=3;s=\"station5\".\"STATE\"")

                        data_station5["STATION5_SEN1"] = s5_sen1.get_value()
                        data_station5["STATION5_SEN2"] = s5_sen2.get_value()
                        data_station5["STATION5_SEN3"] = s5_sen3.get_value()
                        data_station5["STATION5_SEN4"] = s5_sen4.get_value()
                        data_station5["STATION5_SEN5"] = s5_sen5.get_value()
                        data_station5["STATION5_SEN6"] = s5_sen6.get_value()
                        data_station5["STATION5_SEN7"] = s5_sen7.get_value()
                        data_station5["STATION5_M1"] = s5_m1.get_value()
                        data_station5["STATION5_M2"] = s5_m2.get_value()
                        data_station5["NOP"] = s5_NOP.get_value()
                        data_station5["OEE"] = s5_OEE.get_value()
                        data_station5["STATION5_STATE"] = s5_STATE.get_value()
# ********************************************************************************
                        s6_NOP = client.get_node("ns=3;s=\"station6\".\"NOP\"")
                        s6_OEE = client.get_node("ns=3;s=\"station6\".\"OEE\"")
                        s6_STATE = client.get_node("ns=3;s=\"station6\".\"STATE\"")

                        data_station6["NOP"] = s6_NOP.get_value()
                        data_station6["OEE"] = s6_OEE.get_value()
                        data_station6["STATION5_STATE"] = s6_STATE.get_value()
# ********************************************************************************
                        s7_sen1 = client.get_node("ns=3;s=\"station7\".\"sen1\"")
                        s7_m1 = client.get_node("ns=3;s=\"station7\".\"m1\"")
                        s7_NOP = client.get_node("ns=3;s=\"station7\".\"NOP\"")
                        s7_OEE = client.get_node("ns=3;s=\"station7\".\"OEE\"")
                        s7_STATE = client.get_node("ns=3;s=\"station7\".\"STATE\"")

                        data_station7["NOP"] = s7_NOP.get_value()
                        data_station7["OEE"] = s7_OEE.get_value()
                        data_station7["STATION7_STATE"] = s7_STATE.get_value()
                        data_station7["STATION7_SEN1"] = s7_sen1.get_value()
                        data_station7["STATION7_M1"] = s7_m1.get_value()
                        # Tao mot bien kieu du lieu dict de tao chuoi json



                        # *************************************************


                        data_station1_public = json.dumps(data_station1)
                        data_station2_public = json.dumps(data_station2)
                        data_station3_public = json.dumps(data_station3)
                        data_station4_public = json.dumps(data_station4)
                        data_station5_public = json.dumps(data_station5)
                        data_station6_public = json.dumps(data_station6)
                        data_station7_public = json.dumps(data_station7)

                        print(data_station1_public)
                        print(data_station2_public)
                        print(data_station3_public)
                        print(data_station4_public)
                        #*************************************************************************************************

                        print(data_station5_public)
                        #*************************************************************************************************

                        print(data_station6_public)
                        #*************************************************************************************************
                        print(data_station7_public)

                        try:
                                kafka_producer.produce(str(data_station1_public).encode('ascii'))
                                kafka_producer1.produce(str(data_station2_public).encode('ascii'))
                                kafka_producer2.produce(str(data_station3_public).encode('ascii'))
                                kafka_producer3.produce(str(data_station4_public).encode('ascii'))
                                kafka_producer4.produce(str(data_station5_public).encode('ascii'))
                                kafka_producer5.produce(str(data_station6_public).encode('ascii'))
                                kafka_producer6.produce(str(data_station7_public).encode('ascii'))
                                #time.sleep(3)

                                print('Data published')
                                #time.sleep(1)
                        except:
                                print('ERROR WHEN PUBLISH DATA PLEASE CHECK CONNECTION')

        except:
                print('Error when connect please check cable or internet')