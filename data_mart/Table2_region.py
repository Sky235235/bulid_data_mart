import pandas as pd
from tqdm import tqdm
from kakaoapi import KakaoLocalAPI


class ServiceProcessing:

    def __init__(self, service):

        # service 데이터 정의
        self.service = service

    def start_address(self):

        kakao = KakaoLocalAPI()

        start_address = self.service[self.service['출발지 상세'].notna()]
        start_address = start_address.reset_index().drop('index', axis=1)

        s_do_list = []
        s_si_list = []
        s_gu_list = []
        s_dong_list = []

        for i in tqdm(range(len(start_address['출발지 상세']))):

            if '서울' in start_address['출발지 상세'][i]:
                add_split = start_address['출발지 상세'][i].split(' ')

                if len(add_split) <= 2:
                    lng_x = start_address['출발지 경도'][i]
                    lat_y = start_address['출발지 위도'][i]

                    result = kakao.geo_coord2address(lng_x, lat_y, 'WGS84')

                    new_address = result['documents'][0]['address']['address_name']

                    add_split = new_address.split(' ')

                    if add_split[0] == '서울':

                        s_do_list.append('서울특별시')
                        s_si_list.append('서울특별시')
                    else:
                        s_do_list.append(add_split[0])
                        s_si_list.append(add_split[0])


                    s_gu_list.append(add_split[1])
                    s_dong_list.append(add_split[2])

                else:
                    if add_split[0] == '서울':

                        s_do_list.append('서울특별시')
                        s_si_list.append('서울특별시')
                    else:
                        s_do_list.append(add_split[0])
                        s_si_list.append(add_split[0])


                    s_gu_list.append(add_split[1])
                    s_dong_list.append(add_split[2])


            elif '광역시' in start_address['출발지 상세'][i]:
                add_split = start_address['출발지 상세'][i].split(' ')

                if len(add_split) <= 2:
                    lng_x = start_address['출발지 경도'][i]
                    lat_y = start_address['출발지 위도'][i]

                    result = kakao.geo_coord2address(lng_x, lat_y, 'WGS84')

                    new_address = result['documents'][0]['address']['address_name']

                    add_split = new_address.split(' ')

                    s_do_list.append(add_split[0])
                    s_si_list.append(add_split[0])
                    s_gu_list.append(add_split[1])
                    s_dong_list.append(add_split[2])

                else:

                    s_do_list.append(add_split[0])
                    s_si_list.append(add_split[0])
                    s_gu_list.append(add_split[1])
                    s_dong_list.append(add_split[2])

            elif '제주' in start_address['출발지 상세'][i]:
                s_do_list.append('제주특별자치도')
                s_si_list.append('제주시')
                s_gu_list.append('제주시')
                s_dong_list.append('제주시')


            elif len(start_address['출발지 상세'][i].split(' ')) == 2:
                add_split = start_address['출발지 상세'][i].split(' ')
                s_do_list.append(add_split[0])
                s_si_list.append(add_split[0])
                s_gu_list.append(add_split[1])
                s_dong_list.append(add_split[1])

            elif start_address['출발지 상세'][i] == 'None' :

                s_do_list.append('None')
                s_si_list.append('None')
                s_gu_list.append('None')
                s_dong_list.append('None')


            else:
                add_split = start_address['출발지 상세'][i].split(' ')
                if len(add_split) == 1:
                    s_do_list.append('None')
                    s_si_list.append('None')
                    s_gu_list.append('None')
                    s_dong_list.append('None')

                else:

                    if add_split[2].endswith('구'):
                        s_do_list.append(add_split[0])
                        s_si_list.append(add_split[1])
                        s_gu_list.append(add_split[2])
                        s_dong_list.append(add_split[3])

                    else:
                        s_do_list.append(add_split[0])
                        s_si_list.append(add_split[1])
                        s_gu_list.append(add_split[1])
                        s_dong_list.append(add_split[2])


        self.service['출발지_도'] = None
        self.service['출발지_시'] = None
        self.service['출발지_구'] = None
        self.service['출발지_동'] = None

        self.service.loc[self.service['출발지 상세'].notna(), '출발지_도'] = s_do_list
        self.service.loc[self.service['출발지 상세'].notna(), '출발지_시'] = s_si_list
        self.service.loc[self.service['출발지 상세'].notna(), '출발지_구'] = s_gu_list
        self.service.loc[self.service['출발지 상세'].notna(), '출발지_동'] = s_dong_list

        return self.service

    def goal_address(self):

        kakao = KakaoLocalAPI()

        goal_address = self.service[self.service['도착지 상세'].notna()]
        goal_address = goal_address.reset_index().drop('index', axis=1)

        g_do_list = []
        g_si_list = []
        g_gu_list = []
        g_dong_list = []

        for i in tqdm(range(len(goal_address['도착지 상세']))):

            if '서울' in goal_address['도착지 상세'][i]:
                add_split = goal_address['도착지 상세'][i].split(' ')

                if len(add_split) <= 2:

                    lng_x = goal_address['도착지 경도'][i]
                    lat_y = goal_address['도착지 위도'][i]

                    result = kakao.geo_coord2address(lng_x, lat_y, 'WGS84')

                    new_address = result['documents'][0]['address']['address_name']

                    add_split = new_address.split(' ')

                    if add_split[0] == '서울':
                        g_do_list.append('서울특별시')
                        g_si_list.append('서울특별시')

                    else:
                        g_do_list.append(add_split[0])
                        g_si_list.append(add_split[0])

                    g_gu_list.append(add_split[1])
                    g_dong_list.append(add_split[2])

                else:

                    if add_split[0] == '서울':
                        g_do_list.append('서울특별시')
                        g_si_list.append('서울특별시')

                    else:
                        g_do_list.append(add_split[0])
                        g_si_list.append(add_split[0])

                    g_gu_list.append(add_split[1])
                    g_dong_list.append(add_split[2])


            elif '광역시' in goal_address['도착지 상세'][i]:
                add_split = goal_address['도착지 상세'][i].split(' ')

                if len(add_split) <= 2:

                    lng_x = goal_address['도착지 경도'][i]
                    lat_y = goal_address['도착지 위도'][i]

                    result = kakao.geo_coord2address(lng_x, lat_y, 'WGS84')

                    new_address = result['documents'][0]['address']['address_name']

                    add_split = new_address.split(' ')

                    g_do_list.append(add_split[0])
                    g_si_list.append(add_split[0])
                    g_gu_list.append(add_split[1])
                    g_dong_list.append(add_split[2])


                else:

                    g_do_list.append(add_split[0])
                    g_si_list.append(add_split[0])
                    g_gu_list.append(add_split[1])
                    g_dong_list.append(add_split[2])

            elif '제주' in goal_address['도착지 상세'][i]:
                g_do_list.append('제주특별자치도')
                g_si_list.append('제주시')
                g_gu_list.append('제주시')
                g_dong_list.append('제주시')


            elif len(goal_address['도착지 상세'][i].split(' ')) == 2:
                add_split = goal_address['도착지 상세'][i].split(' ')
                g_do_list.append(add_split[0])
                g_si_list.append(add_split[0])
                g_gu_list.append(add_split[1])
                g_dong_list.append(add_split[1])

            elif goal_address['도착지 상세'][i] == 'None':

                g_do_list.append('None')
                g_si_list.append('None')
                g_gu_list.append('None')
                g_dong_list.append('None')

            else:
                add_split = goal_address['도착지 상세'][i].split(' ')

                if len(add_split) == 1:
                    g_do_list.append('None')
                    g_si_list.append('None')
                    g_gu_list.append('None')
                    g_dong_list.append('None')

                else:

                    if add_split[2].endswith('구'):
                        g_do_list.append(add_split[0])
                        g_si_list.append(add_split[1])
                        g_gu_list.append(add_split[2])
                        g_dong_list.append(add_split[3])

                    else:
                        g_do_list.append(add_split[0])
                        g_si_list.append(add_split[1])
                        g_gu_list.append(add_split[1])
                        g_dong_list.append(add_split[2])

        self.service['도착지_도'] = None
        self.service['도착지_시'] = None
        self.service['도착지_구'] = None
        self.service['도착지_동'] = None

        self.service.loc[self.service['도착지 상세'].notna(), '도착지_도'] = g_do_list
        self.service.loc[self.service['도착지 상세'].notna(), '도착지_시'] = g_si_list
        self.service.loc[self.service['도착지 상세'].notna(), '도착지_구'] = g_gu_list
        self.service.loc[self.service['도착지 상세'].notna(), '도착지_동'] = g_dong_list

        return self.service

    # 주행시간
    def trip_time(self):

        trip_time_list = []

        for i in range(len(self.service)):
            diff_trip_time = round((pd.to_datetime(self.service['도착일시'][i]) - pd.to_datetime(self.service['출발일시'][i])).seconds / 60, 2)
            trip_time_list.append(diff_trip_time)

        self.service['주행시간(분)'] = trip_time_list
        self.service['주행시간(분)'] = self.service['주행시간(분)'].fillna(0)

        return self.service

    # 승객도달시간(배차시간)
    def dispatch_time(self):

        dispatch_time_list = []

        for i in range(len(self.service)):
            dispatch_time = round((pd.to_datetime(self.service['출발일시'][i]) - pd.to_datetime(self.service['이용일시'][i])).seconds / 60, 2)
            dispatch_time_list.append(dispatch_time)

        self.service['승객도달시간(분)'] = dispatch_time_list
        self.service['승객도달시간(분)'] = self.service['승객도달시간(분)'].fillna(0)

        return self.service