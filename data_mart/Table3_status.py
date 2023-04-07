import pandas as pd
import numpy as np
from haversine import haversine
from datetime import datetime as dt
from datetime import timedelta
from tqdm import tqdm

class DriverServiceProcessing:

    def __init__(self, service, driver_work_log):
        # service data 정의
        self.service = service
        # 기사로그 데이터 정의
        self.driver_work_log = driver_work_log

    # 하차 상태
    def pickoff_status(self):
        race_finish = self.service[
            (self.service['상태'] == '하차') & ((self.service['구분'] == '앱호출') | (self.service['구분'] == '콜센터')) ]
        race_finish = race_finish.reset_index().drop('index', axis=1)

        finish_dist_list = []
        for i in tqdm(range(len(race_finish))):
            # 배차시 승객과의 거리 계산
            tmp = self.driver_work_log[self.driver_work_log['dispatch_idx'] == race_finish['배차 ID'][i]]
            if 31 in list(tmp['status']):
                start_lat = tmp[tmp['status'] == 31]['lat'].values[0]
                start_lng = tmp[tmp['status'] == 31]['lng'].values[0]

                goal_lat = race_finish['출발지 위도'][i]
                goal_lng = race_finish['출발지 경도'][i]

                start = (start_lat, start_lng)
                goal = (goal_lat, goal_lng)
                finish_dist = haversine(start, goal, unit='m')  # 승객과의 거리

                finish_dist_list.append(finish_dist)

            else:
                finish_dist_list.append(0)

        self.service.loc[(self.service['상태'] == '하차') & ((self.service['구분'] == '앱호출') | (self.service['구분'] == '콜센터')), '배차시 승객과의 거리(m)'] = finish_dist_list

        return self.service

    def user_cancel(self):
        service_cancel = self.service[self.service['상태'] == '배차취소-사용자']
        service_cancel = service_cancel.reset_index().drop('index', axis=1)

        dispatch_dist_list = []
        cancel_dist_list = []
        cancel_time_list = []

        for i in tqdm(range(len(service_cancel))):
            # 배차시 승객과의 거리 계산
            tmp = self.driver_work_log[self.driver_work_log['dispatch_idx'] == service_cancel['배차 ID'][i]]

            if 31 in list(tmp['status']):
                start_lat = tmp[tmp['status'] == 31]['lat'].values[0]
                start_lng = tmp[tmp['status'] == 31]['lng'].values[0]

                goal_lat = service_cancel['출발지 위도'][i]
                goal_lng = service_cancel['출발지 경도'][i]

                start = (start_lat, start_lng)
                goal = (goal_lat, goal_lng)
                dispatch_dist = haversine(start, goal, unit='m')  # 승객과의 거리

                dispatch_dist_list.append(dispatch_dist)

            else:
                dispatch_dist_list.append(0)

            # 배차취소 거리 계산
            if (31 & 33) in tmp['status'].values:
                s_lat = tmp[tmp['status'] == 31]['lat'].values[0]
                s_lng = tmp[tmp['status'] == 31]['lng'].values[0]
                g_lat = tmp[tmp['status'] == 33]['lat'].values[0]
                g_lng = tmp[tmp['status'] == 33]['lng'].values[0]

                start = (s_lat, s_lng)
                goal = (g_lat, g_lng)
                cancel_dist = haversine(start, goal, unit='m')

                cancel_dist_list.append(cancel_dist)

                # 배차취소 시간 계산

                reg_time_33 = tmp[tmp['status'] == 33]['reg_datetime'].values[0]
                reg_time_31 = tmp[tmp['status'] == 31]['reg_datetime'].values[0]

                cancel_time = (reg_time_33 - reg_time_31) / np.timedelta64(1, 's')
                cancel_time_list.append(cancel_time)

            else:
                cancel_dist_list.append(0)
                cancel_time_list.append(0)

        self.service.loc[self.service['상태'] == '배차취소-사용자', '배차시 승객과의 거리(m)'] = dispatch_dist_list
        self.service.loc[self.service['상태'] == '배차취소-사용자', '상태변경까지 걸린시간(s)'] = cancel_time_list
        self.service.loc[self.service['상태'] == '배차취소-사용자', '상태변경까지 걸린거리(m)'] = cancel_dist_list

        return self.service

    def driver_cancel(self):
        driver_cancel = self.service[self.service['상태'] == '배차취소-기사']
        driver_cancel = driver_cancel.reset_index().drop('index', axis=1)

        dispatch_dist_list = []
        cancel_dist_list = []
        cancel_time_list = []

        for i in tqdm(range(len(driver_cancel))):
            # 배차시 승객과의 거리 계산
            tmp = self.driver_work_log[self.driver_work_log['dispatch_idx'] == driver_cancel['배차 ID'][i]]

            if 31 in list(tmp['status']):
                start_lat = tmp[tmp['status'] == 31]['lat'].values[0]
                start_lng = tmp[tmp['status'] == 31]['lng'].values[0]

                goal_lat = driver_cancel['출발지 위도'][i]
                goal_lng = driver_cancel['출발지 경도'][i]

                start = (start_lat, start_lng)
                goal = (goal_lat, goal_lng)
                dispatch_dist = haversine(start, goal, unit='m')  # 승객과의 거리

                dispatch_dist_list.append(dispatch_dist)

            else:
                dispatch_dist_list.append(0)

            # 배차취소 거리 계산
            if (31 & 34) in tmp['status'].values:
                s_lat = tmp[tmp['status'] == 31]['lat'].values[0]
                s_lng = tmp[tmp['status'] == 31]['lng'].values[0]
                g_lat = tmp[tmp['status'] == 34]['lat'].values[0]
                g_lng = tmp[tmp['status'] == 34]['lng'].values[0]

                start = (s_lat, s_lng)
                goal = (g_lat, g_lng)
                cancel_dist = haversine(start, goal, unit='m')

                cancel_dist_list.append(cancel_dist)

                # 배차취소 시간 계산

                reg_time_34 = tmp[tmp['status'] == 34]['reg_datetime'].values[0]
                reg_time_31 = tmp[tmp['status'] == 31]['reg_datetime'].values[0]

                cancel_time = (reg_time_34 - reg_time_31) / np.timedelta64(1, 's')
                cancel_time_list.append(cancel_time)


            else:
                cancel_dist_list.append(0)
                cancel_time_list.append(0)

        self.service.loc[self.service['상태'] == '배차취소-기사', '배차시 승객과의 거리(m)'] = dispatch_dist_list
        self.service.loc[self.service['상태'] == '배차취소-기사', '상태변경까지 걸린시간(s)'] = cancel_time_list
        self.service.loc[self.service['상태'] == '배차취소-기사', '상태변경까지 걸린거리(m)'] = cancel_dist_list

        return self.service

    def g_b_cancel(self):

        g_b_cancel = self.service[self.service['상태'] == '취소']
        g_b_cancel = g_b_cancel.reset_index().drop('index', axis=1)

        cancel_time_list = []
        cancel_dist_list = []
        for i in tqdm(range(len(g_b_cancel))):
            # 시간계산

            # 값 자체가 공백일때
            if list(self.driver_work_log[self.driver_work_log['reg_datetime'] == g_b_cancel['이용일시'][i]][['general_boarding_uuid']].values) == []:
                cancel_time_list.append(0)
                cancel_dist_list.append(0)

            # 일반 주행 ID가 없을 시
            elif list(self.driver_work_log[self.driver_work_log['reg_datetime'] == g_b_cancel['이용일시'][i]].isnull()['general_boarding_uuid'])[0] == True:
                cancel_time_list.append(0)
                cancel_dist_list.append(0)



            else:

                boarding_df = self.driver_work_log[self.driver_work_log['reg_datetime'] == g_b_cancel['이용일시'][i]]

                if (21 in boarding_df['status'].values) & (len(boarding_df[boarding_df['status'] == 21]) == 1):
                    g_b_boarding_id = boarding_df[boarding_df['status'] == 21]['general_boarding_uuid'].values[0]

                    if len(self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id])>1:


                        reg_time_21 = \
                        self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id][
                            'reg_datetime'].values[0]
                        reg_time_23 = \
                        self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id][
                            'reg_datetime'].values[1]

                        cancel_time = (reg_time_23 - reg_time_21) / np.timedelta64(1, 's')  # 상태변경까지 걸린시간(m)
                        cancel_time_list.append(cancel_time)

                        #거리 계산

                        s_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[0]
                        s_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[0]
                        g_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[1]
                        g_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[1]

                    else:
                        reg_time_21 = \
                        self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id][
                            'reg_datetime'].values[0]
                        reg_time_23 = \
                        self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id][
                            'reg_datetime'].values[0]


                        cancel_time = (reg_time_23 - reg_time_21) / np.timedelta64(1, 's')  # 상태변경까지 걸린시간(m)
                        cancel_time_list.append(cancel_time)


                        # 거리계산

                        s_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[0]
                        s_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[0]
                        g_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[0]
                        g_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[0]

                    start = (s_lat, s_lng)
                    goal = (g_lat, g_lng)
                    cancel_dist = haversine(start, goal, unit='m')  # 상태변경까지 걸린거리(m)
                    cancel_dist_list.append(cancel_dist)

                else:

                    g_b_boarding_id = boarding_df['general_boarding_uuid'].dropna().values[-1]

                    reg_time_21 = self.driver_work_log[
                        self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['reg_datetime'].values[0]

                    if len(list(self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['reg_datetime'].values))== 1 :

                        reg_time_23 = reg_time_21

                    else:

                        reg_time_23 = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['reg_datetime'].values[1]

                    cancel_time = (reg_time_23 - reg_time_21) / np.timedelta64(1, 's')  # 상태변경까지 걸린시간(m)
                    cancel_time_list.append(cancel_time)

                    # 거리계산

                    s_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[0]
                    s_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[0]

                    if len(list(self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values))== 1:
                        g_lat = s_lat
                        g_lng = s_lng

                    else:

                        g_lat = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lat'].values[1]
                        g_lng = self.driver_work_log[self.driver_work_log['general_boarding_uuid'] == g_b_boarding_id]['lng'].values[1]

                    start = (s_lat, s_lng)
                    goal = (g_lat, g_lng)
                    cancel_dist = haversine(start, goal, unit='m')  # 상태변경까지 걸린거리(m)
                    cancel_dist_list.append(cancel_dist)

        self.service.loc[self.service['상태'] == '취소', '상태변경까지 걸린시간(s)'] = cancel_time_list
        self.service.loc[self.service['상태'] == '취소', '상태변경까지 걸린거리(m)'] = cancel_dist_list

        return self.service

    def race_stop(self):
        race_stop = self.service[self.service['상태'] == '주행중지']
        race_stop = race_stop.reset_index().drop('index', axis=1)

        dispatch_dist_list = []
        stop_time_list = []
        stop_dist_list = []

        for i in tqdm(range(len(race_stop))):
            # 배차시 승객과의 거리 계산
            dispatch_df = self.driver_work_log[self.driver_work_log['dispatch_idx'] == race_stop['배차 ID'][i]]

            if 31 in list(dispatch_df['status']):

                s_lat = dispatch_df[dispatch_df['status'] == 31]['lat'].values[0]
                s_lng = dispatch_df[dispatch_df['status'] == 31]['lng'].values[0]

                g_lat = race_stop['출발지 위도'][i]
                g_lng = race_stop['출발지 경도'][i]

                start = (s_lat, s_lng)
                goal = (g_lat, g_lng)
                dispatch_dist = haversine(start, goal, unit='m')  # 승객과의 거리

                dispatch_dist_list.append(dispatch_dist)

            else:
                dispatch_dist_list.append(0)


            # 주행중지 시간 계산
            if (36 in dispatch_df['status'].values) & (38 in dispatch_df['status'].values):

                reg_time_38 = dispatch_df[dispatch_df['status'] == 38]['reg_datetime'].values[0]
                reg_time_36 = dispatch_df[dispatch_df['status'] == 36]['reg_datetime'].values[0]

                stop_time = (reg_time_38 - reg_time_36) / np.timedelta64(1, 's')
                stop_time_list.append(stop_time)

                # 상태변경까지 거리계산
                s_lat = dispatch_df[dispatch_df['status'] == 36]['lat'].values[0]
                s_lng = dispatch_df[dispatch_df['status'] == 36]['lng'].values[0]
                g_lat = dispatch_df[dispatch_df['status'] == 38]['lat'].values[0]
                g_lng = dispatch_df[dispatch_df['status'] == 38]['lng'].values[0]

                s = (s_lat, s_lng)
                g = (g_lat, g_lng)
                stop_dist = haversine(s, g, unit='m')

                stop_dist_list.append(stop_dist)
            else:
                stop_time_list.append(0)
                stop_dist_list.append(0)

        self.service.loc[self.service['상태'] == '주행중지', '배차시 승객과의 거리(m)'] = dispatch_dist_list
        self.service.loc[self.service['상태'] == '주행중지', '상태변경까지 걸린시간(s)'] = stop_time_list
        self.service.loc[self.service['상태'] == '주행중지', '상태변경까지 걸린거리(m)'] = stop_dist_list

        return self.service

    def no_show(self):

        no_board = self.service[self.service['상태'] == '미탑승신고']
        no_board = no_board.reset_index().drop('index', axis=1)

        dispatch_dist_list = []
        no_board_time_list = []
        no_board_dist_list = []

        for i in tqdm(range(len(no_board))):
            # 배차시 승객과의 거리 계산
            dispatch_df = self.driver_work_log[self.driver_work_log['dispatch_idx'] == no_board['배차 ID'][i]]
            if 31 in list(dispatch_df['status']):

                s_lat = dispatch_df[dispatch_df['status'] == 31]['lat'].values[0]
                s_lng = dispatch_df[dispatch_df['status'] == 31]['lng'].values[0]

                g_lat = no_board['출발지 위도'][i]
                g_lng = no_board['출발지 경도'][i]

                start = (s_lat, s_lng)
                goal = (g_lat, g_lng)
                dispatch_dist = haversine(start, goal, unit='m')  # 승객과의 거리

                dispatch_dist_list.append(dispatch_dist)

            else:
                dispatch_dist_list.append(0)

            if (36 in dispatch_df['status'].values) & (37 in dispatch_df['status'].values):

                # 주행중지 시간 계산
                reg_time_37 = dispatch_df[dispatch_df['status'] == 37]['reg_datetime'].values[0]
                reg_time_36 = dispatch_df[dispatch_df['status'] == 36]['reg_datetime'].values[0]

                no_board_time = (reg_time_37 - reg_time_36) / np.timedelta64(1, 's')
                no_board_time_list.append(no_board_time)

                # 상태변경까지 거리계산
                s_lat = dispatch_df[dispatch_df['status'] == 36]['lat'].values[0]
                s_lng = dispatch_df[dispatch_df['status'] == 36]['lng'].values[0]
                g_lat = dispatch_df[dispatch_df['status'] == 37]['lat'].values[0]
                g_lng = dispatch_df[dispatch_df['status'] == 37]['lng'].values[0]

                s = (s_lat, s_lng)
                g = (g_lat, g_lng)
                no_board_dist = haversine(s, g, unit='m')

                no_board_dist_list.append(no_board_dist)

            else:
                no_board_time_list.append(0)
                no_board_dist_list.append(0)

        self.service.loc[self.service['상태'] == '미탑승신고', '배차시 승객과의 거리(m)'] = dispatch_dist_list
        self.service.loc[self.service['상태'] == '미탑승신고', '상태변경까지 걸린시간(s)'] = no_board_time_list
        self.service.loc[self.service['상태'] == '미탑승신고', '상태변경까지 걸린거리(m)'] = no_board_dist_list

        return self.service
