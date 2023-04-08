import math
import pandas as pd
import pymysql
from tqdm import tqdm

class DBupdate:
    def __init__(self):
        # # 배차 Dev server 접근
        host = '*'
        username = '*'
        password = '*'
        port = '*'
        database = '*'

        self.conn = pymysql.connect(host=host, user=username, db=database, port=port,
                                    password=password)
        self.conn.cursor(pymysql.cursors.DictCursor)
    #소멸자
    def __del__(self):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()

    def table1_update(self, data):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)

        query = """INSERT INTO `pos`.`im_service_section_info`(**)
           values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in tqdm(range(len(data))):

            boarding_datetime = data['이용일시'][i]
            dispatch_idx = data['배차 ID'][i]
            boarding_type = data['서비스'][i]
            boarding_name = data['구분'][i]
            status = data['상태'][i]

            company_name = data['택시회사'][i]

            if math.isnan(data['기사 일련번호'][i]) == True:
                driver_idx = None

            else:
                driver_idx = data['기사 일련번호'][i]

            car_number = data['차량번호'][i]
            phone_number = data['고객 전화번호'][i]
            departure_lat = data['출발지 위도'][i]
            departure_lng = data['출발지 경도'][i]
            departure_address = data['출발지'][i]
            departure_address_detail = data['출발지 상세'][i]

            if type(data['출발일시'][i]) == pd._libs.tslibs.nattype.NaTType:
                departure_datetime = None

            else:
                departure_datetime = data['출발일시'][i]

            if math.isnan(data['도착지 위도'][i]) == True:
                arrival_lat = 0

            else:
                arrival_lat = data['도착지 위도'][i]

            if math.isnan(data['도착지 경도'][i]) == True:
                arrival_lng = 0

            else:
                arrival_lng = data['도착지 경도'][i]

            arrival_address = data['도착지'][i]
            arrival_address_detail = data['도착지 상세'][i]

            if type(data['도착일시'][i]) == pd._libs.tslibs.nattype.NaTType:
                arrival_datetime = None

            else:
                arrival_datetime = data['도착일시'][i]

            payment_type = data['결제방법'][i]
            fare = data['요금'][i]
            toll = data['통행료'][i]
            additional_amount = data['추가금액'][i]
            discount_amount = data['할인금액'][i]
            use_point = data['사용 포인트'][i]
            section = data['section'][i]
            g_section = data['gsection'][i]

            curs.execute(query, (boarding_datetime, dispatch_idx, boarding_type, boarding_name, status, company_name,
                                 driver_idx, car_number, phone_number, departure_lat, departure_lng, departure_address,
                                 departure_address_detail, departure_datetime, arrival_lat, arrival_lng,
                                 arrival_address, arrival_address_detail,
                                 arrival_datetime, payment_type, fare, toll, additional_amount, discount_amount,
                                 use_point, section, g_section))

            self.conn.commit()

    def table2_update(self, data):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)

        query = """INSERT INTO `pos`.`im_service_car_log_info`(**)
           values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in tqdm(range(len(data))):
            boarding_datetime = data['이용일시'][i]
            dispatch_idx = data['배차 ID'][i]
            general_boarding_uuid = data['general_boarding_uuid'][i]
            boarding_day = data['일'][i]
            boarding_hour = data['시간'][i]
            boarding_week = data['요일'][i]
            boarding_fare = data['요금합계'][i]

            if data['예상금액'][i] < 0:
                estimated_amount = 0

            else:
                estimated_amount = data['예상금액'][i]

            reservation_fare = data['대절요금'][i]
            reservation_fee_fare = data['취소수수료'][i]
            departure_do = data['출발지_도'][i]
            departure_gu = data['출발지_구'][i]
            departure_city = data['출발지_시'][i]
            departure_dong = data['출발지_동'][i]
            arrival_do = data['도착지_도'][i]
            arrival_gu = data['도착지_구'][i]
            arrival_city = data['도착지_시'][i]
            arrival_dong = data['도착지_동'][i]

            # 승객도달시간
            if math.isnan(data['승객도달시간(분)'][i]) == True:
                user_arrival_time = None

            else:
                user_arrival_time = data['승객도달시간(분)'][i]

            # 주행시간
            if math.isnan(data['주행시간(분)'][i]) == True:
                boarding_time = None

            else:
                boarding_time = data['주행시간(분)'][i]

            # 주행거리 아래에 예상거리 추가
            if math.isnan(data['주행거리'][i]) == True:
                driving_distance = None

            else:
                driving_distance = data['주행거리'][i]

            if math.isnan(data['예상거리'][i]) == True:
                estimated_distance = None

            else:
                estimated_distance = data['예상거리'][i]

            curs.execute(query, (boarding_datetime, dispatch_idx,general_boarding_uuid, boarding_day, boarding_hour, boarding_week, boarding_fare,estimated_amount,
                                 reservation_fare, reservation_fee_fare,departure_do,departure_gu, departure_city, departure_dong,
                                 arrival_do, arrival_gu, arrival_city, arrival_dong, user_arrival_time,
                                boarding_time, driving_distance,estimated_distance))

            self.conn.commit()

    def table3_update(self, data):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)

        query = """INSERT INTO `pos`.`im_service_driver_log_info`(**)
           values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in tqdm(range(len(data))):
            boarding_datetime = data['이용일시'][i]
            dispatch_idx = data['배차 ID'][i]
            general_boarding_uuid = data['general_boarding_uuid'][i]
            is_call = data['is_call'][i]

            if math.isnan(data['company_idx'][i]) == True:
                company_idx = None

            else:
                company_idx = data['company_idx'][i]



            if math.isnan(data['car_type'][i]) == True:
                car_type_idx = None

            else:
                car_type_idx = data['car_type'][i]

            dispatch_type = data['dispatch_type'][i]
            dispatch_distance = data['배차시 승객과의 거리(m)'][i]
            status_change_time = data['상태변경까지 걸린시간(s)'][i]
            status_change_distance = data['상태변경까지 걸린거리(m)'][i]

            curs.execute(query, (boarding_datetime, dispatch_idx, general_boarding_uuid,is_call, company_idx,
                                 car_type_idx, dispatch_type,dispatch_distance, status_change_time,status_change_distance))

            self.conn.commit()








