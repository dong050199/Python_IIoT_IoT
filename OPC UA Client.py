from opcua import Client
from pykafka import KafkaClient
import time
import json


def ConvertStringToByte_StationState(StringValue):
        if StringValue == "RDLOAD":
                value = 1
        elif StringValue == "LOAD":
                value = 2
        elif StringValue == "PROCESS":
                value = 3
        elif StringValue == "FINISH":
                value = 4
        elif StringValue == "RDPICK":
                value = 5
        else:
                value = None
        return value


def ConvertStringToByte_MotorState(StringValue):
        if StringValue == "RUN":
                value = 1
        if StringValue == "STOP":
                value = 2
        else:
                value = 3
        return value


while True:
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
                        url = "opc.tcp://127.0.0.1:4840"
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

                Station1_NOP = 0
                Station2_NOP = 0
                Station3_NOP = 0
                Station4_NOP = 0
                Station5_NOP = 0
                Station7_NOP = 0
                while True:

                        prescenarios = client.get_node("ns=2;i=1")
                        scenarios = prescenarios.get_value()

                        ################## Calculator State System for Display ##################
                        #                                                                       #
                        ################################ STATION1 ###############################

                        if scenarios == "0:0":
                                Station1_state = "RDLOAD"
                        elif scenarios == "2:0":
                                Station1_state = "LOAD"
                        elif scenarios == "4:0":
                                Station1_state = "PROCESS"
                        elif scenarios == "5:0":
                                Station1_state = "FINISH"
                                Station1_NOP = Station1_NOP + 1
                        elif scenarios == "6:0":
                                Station1_state = "RDPICK"
                        else:
                                Station1_state = "RDLOAD"
                        ################################ STATION2 ###############################
                        if scenarios == "7:0":
                                Station2_state = "RDLOAD"
                        elif scenarios == "8:0":
                                Station2_state = "LOAD"
                        elif scenarios == "9:0":
                                Station2_state = "PROCESS"
                        elif scenarios == "12:0":
                                Station2_state = "FINISH"
                                Station2_NOP = Station2_NOP + 1
                        elif scenarios == "13:0":
                                Station2_state = "RDPICK"
                        else:
                                Station2_state = "RDLOAD"
                        ################################ STATION3 ###############################
                        if scenarios == "14:0":
                                Station3_state = "RDLOAD"
                        elif scenarios == "16:0":
                                Station3_state = "LOAD"
                        elif scenarios == "17:0":
                                Station3_state = "PROCESS"
                        elif scenarios == "19:0":
                                Station3_state = "FINISH"
                                Station3_NOP = Station3_NOP + 1
                        elif scenarios == "20:0":
                                Station3_state = "RDPICK"
                        else:
                                Station3_state = "RDLOAD"


                        ################################ STATION4 ###############################
                        if scenarios == "21:0":
                                Station4_state = "RDLOAD"
                        elif scenarios == "22:0":
                                Station4_state = "LOAD"
                        elif scenarios == "26:0":
                                Station4_state = "PROCESS"
                        elif scenarios == "31:0":
                                Station4_state = "FINISH"
                                Station4_NOP = Station4_NOP + 1
                        elif scenarios == "36:0":
                                Station4_state = "RDPICK"
                        else:
                                Station4_state = "RDLOAD"


                        ################################ STATION5 ###############################
                        if scenarios == "37:0":
                                Station5_state = "RDLOAD"
                        elif scenarios == "40:0":
                                Station5_state = "LOAD"
                        elif scenarios == "174:0":
                                Station5_state = "PROCESS"
                        elif scenarios == "196:0":
                                Station5_state = "FINISH"
                                Station5_NOP = Station5_NOP + 1
                        elif scenarios == "198:0":
                                Station5_state = "RDPICK"
                        else:
                                Station5_state = "RDLOAD"
                        ################################ STATION6 ###############################
                        if scenarios == "1001:0":
                                Station6_state = 11
                        elif scenarios == "1008:0":
                                Station6_state = 12
                        elif scenarios == "2001:0":
                                Station6_state = 21
                        elif scenarios == "2008:0":
                                Station6_state = 22
                        elif scenarios == "3001:0":
                                Station6_state = 31
                        elif scenarios == "3049:0":
                                Station6_state = 32
                        elif scenarios == "4001:0":
                                Station6_state = 41
                        elif scenarios == "4028:0":
                                Station6_state = 42
                        elif scenarios == "5002:0":
                                Station6_state = 51
                        elif scenarios == "5061:0":
                                Station6_state = 52
                        elif scenarios == "5061:0":
                                Station6_state = 61
                        elif scenarios == "5062:0":
                                Station6_state = 62
                        else:
                                Station6_state = 11
                        ################################ STATION7 ###############################
                        if scenarios == "1000":
                                Station7_state = "RDLOAD"
                        elif scenarios == "2000":
                                Station7_state = "LOAD"
                        elif scenarios == "2000":
                                Station7_state = "PROCESS"
                        elif scenarios == "2000":
                                Station7_state = "FINISH"
                                Station7_NOP = Station7_NOP + 1
                        elif scenarios == "2000":
                                Station7_state = "RDPICK"
                        else:
                                Station7_state = "RDLOAD"



                        s1_m1_state = client.get_node("ns=2;i=14").get_value()
                        s1_sen1_value = client.get_node("ns=2;i=9").get_value()
                        s1_sen2_value = client.get_node("ns=2;i=11").get_value()

                        data_station1["STATION1_SEN1"] = s1_sen1_value
                        data_station1["STATION1_SEN2"] = s1_sen2_value
                        data_station1["STATION1_M1"] = ConvertStringToByte_MotorState(s1_m1_state)


                        s2_m1_state = client.get_node("ns=2;i=23").get_value()
                        s2_sen1_value = client.get_node("ns=2;i=18").get_value()
                        s2_sen2_value = client.get_node("ns=2;i=20").get_value()


                        data_station2["STATION2_SEN1"] = s2_sen1_value
                        data_station2["STATION2_SEN2"] = s2_sen2_value
                        data_station2["STATION2_M1"] = ConvertStringToByte_MotorState(s2_m1_state)

                        s3_m1_state = client.get_node("ns=2;i=32").get_value()
                        s3_sen1_value = client.get_node("ns=2;i=27").get_value()
                        s3_sen2_value = client.get_node("ns=2;i=29").get_value()

                        data_station3["STATION3_SEN1"] = s2_sen1_value
                        data_station3["STATION3_SEN2"] = s2_sen2_value
                        data_station3["STATION3_M1"] = ConvertStringToByte_MotorState(s3_m1_state)

                        s4_m1_state = client.get_node("ns=2;i=45").get_value()
                        s4_m2_state = client.get_node("ns=2;i=47").get_value()
                        s4_sen1_value = client.get_node("ns=2;i=36").get_value()
                        s4_sen2_value = client.get_node("ns=2;i=38").get_value()
                        s4_sen3_value = client.get_node("ns=2;i=40").get_value()
                        s4_sen4_value = client.get_node("ns=2;i=42").get_value()

                        data_station4["STATION4_SEN1"] = s4_sen1_value
                        data_station4["STATION4_SEN2"] = s4_sen2_value
                        data_station4["STATION4_SEN3"] = s4_sen3_value
                        data_station4["STATION4_SEN4"] = s4_sen4_value
                        data_station4["STATION4_M1"] = ConvertStringToByte_MotorState(s4_m1_state)
                        data_station4["STATION4_M2"] = ConvertStringToByte_MotorState(s4_m2_state)

                        s5_m1_state = client.get_node("ns=2;i=66").get_value()
                        s5_m2_state = client.get_node("ns=2;i=68").get_value()
                        s5_m3_state = client.get_node("ns=2;i=70").get_value()
                        s5_m4_state = client.get_node("ns=2;i=72").get_value()
                        s5_m5_state = client.get_node("ns=2;i=74").get_value()
                        s5_sen1_value = client.get_node("ns=2;i=51").get_value()
                        s5_sen2_value = client.get_node("ns=2;i=53").get_value()
                        s5_sen3_value = client.get_node("ns=2;i=55").get_value()
                        s5_sen4_value = client.get_node("ns=2;i=57").get_value()
                        s5_sen5_value = client.get_node("ns=2;i=59").get_value()
                        s5_sen6_value = client.get_node("ns=2;i=61").get_value()
                        s5_sen7_value = client.get_node("ns=2;i=63").get_value()
                        # s5_servo1_deg = client.get_node("ns=2;id=77").get_value()
                        # s5_servo2_deg = client.get_node("ns=2;id=79").get_value()


                        data_station5["STATION5_SEN1"] = s5_sen1_value
                        data_station5["STATION5_SEN2"] = s5_sen2_value
                        data_station5["STATION5_SEN3"] = s5_sen3_value
                        data_station5["STATION5_SEN4"] = s5_sen4_value
                        data_station5["STATION5_SEN5"] = s5_sen5_value
                        data_station5["STATION5_SEN6"] = s5_sen6_value
                        data_station5["STATION5_SEN7"] = s5_sen7_value
                        data_station5["STATION5_M1"] = ConvertStringToByte_MotorState(s5_m1_state)
                        data_station5["STATION5_M2"] = ConvertStringToByte_MotorState(s5_m2_state)
                        data_station5["STATION5_M3"] = ConvertStringToByte_MotorState(s5_m3_state)
                        data_station5["STATION5_M4"] = ConvertStringToByte_MotorState(s5_m4_state)
                        data_station5["STATION5_M5"] = ConvertStringToByte_MotorState(s5_m5_state)

                        # s6_robot1_B = Client.get_node("ns=2;id=88").get_value()
                        # s6_robot1_pos_S = Client.get_node("ns=2;id=87").get_value()
                        # s6_robot1_pos_X = Client.get_node("ns=2;id=84").get_value()
                        # s6_robot1_pos_Y = Client.get_node("ns=2;id=85").get_value()
                        # s6_robot1_pos_Z = Client.get_node("ns=2;id=86").get_value()
                        # s6_robot1_V = Client.get_node("ns=2;id=89").get_value()
                        # s6_robot1_state = Client.get_node("ns=2;id=90").get_value()

                        s7_m1_state = client.get_node("ns=2;i=97").get_value()
                        s7_sen1_value = client.get_node("ns=2;i=94").get_value()

                        data_station7["STATION5_M7"] = ConvertStringToByte_MotorState(s7_m1_state)
                        data_station7["STATION7_SEN1"] = s7_sen1_value

                        s1_STATE = ConvertStringToByte_StationState(Station1_state)
                        s2_STATE = ConvertStringToByte_StationState(Station2_state)
                        s3_STATE = ConvertStringToByte_StationState(Station3_state)
                        s4_STATE = ConvertStringToByte_StationState(Station4_state)
                        s5_STATE = ConvertStringToByte_StationState(Station5_state)
                        s7_STATE = ConvertStringToByte_StationState(Station7_state)

                        data_station1["STATION1_STATE"] = s1_STATE
                        data_station2["STATION2_STATE"] = s2_STATE
                        data_station3["STATION3_STATE"] = s3_STATE
                        data_station4["STATION4_STATE"] = s4_STATE
                        data_station5["STATION5_STATE"] = s5_STATE
                        data_station6["STATION6_STATE"] = Station6_state
                        data_station7["STATION7_STATE"] = s7_STATE

                        data_station1_public = json.dumps(data_station1)
                        data_station2_public = json.dumps(data_station2)
                        data_station3_public = json.dumps(data_station3)
                        data_station4_public = json.dumps(data_station4)
                        data_station5_public = json.dumps(data_station5)
                        data_station6_public = json.dumps(data_station6)
                        data_station7_public = json.dumps(data_station7)

                        print(data_station1_public)
                        # *************************************************************************************************
                        print(data_station2_public)
                        # *************************************************************************************************
                        print(data_station3_public)
                        # *************************************************************************************************
                        print(data_station4_public)
                        # *************************************************************************************************
                        print(data_station5_public)
                        # *************************************************************************************************
                        print(data_station6_public)
                        # *************************************************************************************************
                        print(data_station7_public)
                        #***********************************************************
                        print(data_station1_public)

                        kafka_producer.produce(str(data_station1_public).encode('ascii'))
                        kafka_producer1.produce(str(data_station2_public).encode('ascii'))
                        kafka_producer2.produce(str(data_station3_public).encode('ascii'))
                        kafka_producer3.produce(str(data_station4_public).encode('ascii'))
                        kafka_producer4.produce(str(data_station5_public).encode('ascii'))
                        kafka_producer5.produce(str(data_station6_public).encode('ascii'))
                        kafka_producer6.produce(str(data_station7_public).encode('ascii'))
                        # time.sleep(3)

                        print('Data published')
