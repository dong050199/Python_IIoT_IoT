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
                        ##########################STATION1##############################
                        s1_m1_state = client.get_node("ns=2;i=14")
                        print(s1_m1_state.get_value())
                        s1_sen1_value = client.get_node("ns=2;i=9")
                        s1_sen2_value = client.get_node("ns=2;i=11")
                        ##########################STATION2##############################
                        s2_m1_state = client.get_node("ns=2;i=23")
                        s2_sen1_value = client.get_node("ns=2;i=18")
                        s2_sen2_value = client.get_node("ns=2;i=20")
                        ##########################STATION3##############################
                        s3_m1_state = client.get_node("ns=2;i=32")
                        s3_sen1_value = client.get_node("ns=2;i=27")
                        s3_sen2_value = client.get_node("ns=2;i=29")
                        ##########################STATION4##############################
                        s4_m1_state = client.get_node("ns=2;i=45")
                        s4_m2_state = client.get_node("ns=2;i=47")
                        s4_sen1_value = client.get_node("ns=2;i=36")
                        s4_sen2_value = client.get_node("ns=2;i=38")
                        s4_sen3_value = client.get_node("ns=2;i=40")
                        s4_sen4_value = client.get_node("ns=2;i=42")
                        ##########################STATION5##############################
                        s5_m1_state = client.get_node("ns=2;i=66")
                        s5_m2_state = client.get_node("ns=2;i=68")
                        s5_m1_state = client.get_node("ns=2;i=70")
                        s5_m2_state = client.get_node("ns=2;i=72")
                        s5_m1_state = client.get_node("ns=2;i=74")
                        s5_sen1_value = client.get_node("ns=2;i=51")
                        s5_sen2_value = client.get_node("ns=2;i=53")
                        s5_sen3_value = client.get_node("ns=2;i=55")
                        s5_sen4_value = client.get_node("ns=2;i=57")
                        s5_sen5_value = client.get_node("ns=2;i=59")
                        s5_sen6_value = client.get_node("ns=2;i=61")
                        s5_sen7_value = client.get_node("ns=2;i=63")
                        s5_servo1_deg = client.get_node("ns=2;id=77")
                        s5_servo2_deg = client.get_node("ns=2;id=79")
                        ##########################STATION6##############################
                        s6_robot1_B = Client.get_node("ns=2;id=88")
                        s6_robot1_pos_S = Client.get_node("ns=2;id=87")
                        s6_robot1_pos_X = Client.get_node("ns=2;id=84")
                        s6_robot1_pos_Y = Client.get_node("ns=2;id=85")
                        s6_robot1_pos_Z = Client.get_node("ns=2;id=86")
                        s6_robot1_V = Client.get_node("ns=2;id=89")
                        s6_robot1_state = Client.get_node("ns=2;id=90")
                        ##########################STATION7##############################
                        s7_m1_state = client.get_node("ns=2;i=97")
                        s7_sen1_value = client.get_node("ns=2;i=94")
                        ##########################Scenarios##############################
                        scenarios = client.get_node("ns=2;i=1")
                        ###############ConvertDataToDisplayDashboard#####################
                        # THẰNG NÀO LÀ SENSORS THÌ ĐỂ Y CHANG VÌ NÓ LÀ DẠNG BOOLEAN RỒI
                        # THẰNG NÀO LÀ MOTOR THÌ NÓ LÀ STOP VÀ VÀ VÀ :DDD
                        # I THINK THE TAG FOR MOTOR STATE NOW IS STRING AND IF I WANT TU USE FOR DISPLAY I NEED TO CONVERT TO BYTE VALUE AND HOW?
                        #ALSO WHAT THE VALUE FOR NUMBER OF PRODUCT?
                        # SO I THINK THAT WE NEED TO USE THE SCENE STATE FOR CHANGE ALL STATE FOR ALL STATION? WHAT WRONG AND WHAT TRUE?
                        #I DONT KNOW BUT I THINK I HAVE SOME MISTMAKE WHEN CODING :DDD
                        #NOW YOU NEED TO USE THE FUNCTION OR SOMETHINGS LIKE THAT FOR MAKE YOUR FIT FOR THE DASHBOARD

                        def ConvertStringToByte_MotorState(StringValue):
                                if StringValue == "RUN":
                                        value = 1
                                if StringValue == "STOP":
                                        value=2
                                if StringValue == "ALARM":
                                        value=3
                                return value
                        def ConvertStringToByte_StationState(StringValue):
                                if StringValue == "RDLOAD":
                                        value = 1
                                if StringValue == "LOAD":
                                        value = 2
                                if StringValue == "PROCESS":
                                        value =3
                                if StringValue == "FINISH":
                                        value = 4;
                                if StringValue == "RDPICK":
                                        value = 5;
                                return value


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
                        ################################ STATION2 ###############################
                        elif scenarios == "7:0":
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
                        ################################ STATION3 ###############################
                        elif scenarios == "14:0":
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


                        ################################ STATION4 ###############################
                        elif scenarios == "21:0":
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


                        ################################ STATION5 ###############################
                        elif scenarios == "37:0":
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
                        ################################ STATION6 ###############################
                        elif scenarios == "1001:0":
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
                        ################################ STATION7 ###############################
                        elif scenarios == "1000":
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



                        s1_sen1 = s1_sen1_value
                        s1_sen2 = s1_sen2_value
                        s1_m1 = ConvertStringToByte_MotorState(s1_m1_state)
                        s1_NOP = Station1_NOP
                        #s1_OEE = client.get_node("ns=3;s=\"station1\".\"OEE\"")
                        s1_STATE = ConvertStringToByte_StationState(Station1_state)

                        data_station1["STATION1_SEN1"] = s1_sen1.get_value()
                        data_station1["STATION1_SEN2"] = s1_sen2.get_value()
                        data_station1["STATION1_M1"] = s1_m1
                        data_station1["NOP"] = s1_NOP
                        #data_station1["OEE"] = s1_OEE.get_value()
                        data_station1["STATION1_STATE"] = s1_STATE
#********************************************************************************
                        s2_sen1 = s2_sen1_value
                        s2_sen2 = s2_sen2_value
                        s2_m1 = ConvertStringToByte_MotorState(s2_m1_state)
                        s2_NOP = Station2_NOP
                        #s2_OEE = client.get_node("ns=3;s=\"station2\".\"OEE\"")
                        s2_STATE = ConvertStringToByte_StationState(Station2_state)

                        data_station2["STATION2_SEN1"] = s2_sen1.get_value()
                        data_station2["STATION2_SEN2"] = s2_sen2.get_value()
                        data_station2["STATION2_M1"] = s2_m1
                        data_station2["NOP"] = s2_NOP
                        #data_station2["OEE"] = s2_OEE.get_value()
                        data_station2["STATION2_STATE"] = s2_STATE
# ********************************************************************************
                        s3_sen1 = s3_sen1_value
                        s3_sen2 = s3_sen2_value
                        s3_m1 = ConvertStringToByte_MotorState(s3_m1_state)
                        s3_NOP = Station3_NOP
                        #s3_OEE = client.get_node("ns=3;s=\"station3\".\"OEE\"")
                        s3_STATE = ConvertStringToByte_StationState(Station3_state)

                        data_station3["STATION3_SEN1"] = s3_sen1.get_value()
                        data_station3["STATION3_SEN2"] = s3_sen2.get_value()
                        data_station3["STATION3_M1"] = s3_m1
                        data_station3["NOP"] = s3_NOP
                        #data_station3["OEE"] = s3_OEE.get_value()
                        data_station3["STATION3_STATE"] = s3_STATE
# ********************************************************************************
                        s4_sen1 = s4_sen1_value
                        s4_sen2 = s4_sen2_value
                        s4_sen3 = s4_sen3_value
                        s4_sen4 = s4_sen4_value
                        s4_m1 = ConvertStringToByte_MotorState(s4_m1_state)
                        s4_m2 = ConvertStringToByte_MotorState(s4_m2_state)
                        s4_NOP = Station4_NOP
                        #s4_OEE = client.get_node("ns=3;s=\"station4\".\"OEE\"")
                        s4_STATE = ConvertStringToByte_StationState(Station4_state)

                        data_station4["STATION4_SEN1"] = s4_sen1.get_value()
                        data_station4["STATION4_SEN2"] = s4_sen2.get_value()
                        data_station4["STATION4_SEN3"] = s4_sen3.get_value()
                        data_station4["STATION4_SEN4"] = s4_sen4.get_value()
                        data_station4["STATION4_M1"] = s4_m1
                        data_station4["STATION4_M2"] = s4_m2
                        data_station4["NOP"] = s4_NOP
                        #data_station4["OEE"] = s4_OEE.get_value()
                        data_station4["STATION4_STATE"] = s4_STATE
# ********************************************************************************
                        s5_sen1 = s5_sen1_value
                        s5_sen2 = s5_sen2_value
                        s5_sen3 = s5_sen3_value
                        s5_sen4 = s5_sen4_value
                        s5_sen5 = s5_sen5_value
                        s5_sen6 = s5_sen6_value
                        s5_sen7 = s5_sen7_value
                        s5_m1 = ConvertStringToByte_MotorState(s5_m1_state)
                        s5_m2 = ConvertStringToByte_MotorState(s5_m2_state)
                        s5_NOP = Station5_NOP
                        #s5_OEE = client.get_node("ns=3;s=\"station5\".\"OEE\"")
                        s5_STATE = ConvertStringToByte_StationState(Station5_state)

                        data_station5["STATION5_SEN1"] = s5_sen1.get_value()
                        data_station5["STATION5_SEN2"] = s5_sen2.get_value()
                        data_station5["STATION5_SEN3"] = s5_sen3.get_value()
                        data_station5["STATION5_SEN4"] = s5_sen4.get_value()
                        data_station5["STATION5_SEN5"] = s5_sen5.get_value()
                        data_station5["STATION5_SEN6"] = s5_sen6.get_value()
                        data_station5["STATION5_SEN7"] = s5_sen7.get_value()
                        data_station5["STATION5_M1"] = s5_m1
                        data_station5["STATION5_M2"] = s5_m2
                        data_station5["NOP"] = s5_NOP
                        #data_station5["OEE"] = s5_OEE.get_value()
                        data_station5["STATION5_STATE"] = s5_STATE
# ********************************************************************************
                        #s6_NOP = Station6_NOP
                        #s6_OEE = client.get_node("ns=3;s=\"station6\".\"OEE\"")
                        #s6_STATE = client.get_node("ns=3;s=\"station6\".\"STATE\"")

                        #data_station6["NOP"] = s6_NOP.get_value()
                        #data_station6["OEE"] = s6_OEE.get_value()
                        data_station6["STATION6_STATE"] = Station6_state
# ********************************************************************************
                        s7_sen1 = s7_sen1_value
                        s7_m1 = ConvertStringToByte_MotorState(s7_m1_state)
                        s7_NOP = Station7_NOP
                        #s7_OEE = client.get_node("ns=3;s=\"station7\".\"OEE\"")
                        s7_STATE = ConvertStringToByte_StationState(Station7_state)

                        data_station7["NOP"] = s7_NOP
                        #data_station7["OEE"] = s7_OEE.get_value()
                        data_station7["STATION7_STATE"] = s7_STATE
                        data_station7["STATION7_SEN1"] = s7_sen1.get_value()
                        data_station7["STATION7_M1"] = s7_m1
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
                        # *************************************************************************************************
                        print(data_station2_public)
                        # *************************************************************************************************
                        print(data_station3_public)
                        # *************************************************************************************************
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
                        except :
                                print('ERROR WHEN PUBLISH DATA PLEASE CHECK CONNECTION')
