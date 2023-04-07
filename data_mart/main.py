# 1. 라이브러리 불러오기
from pyproj import Transformer
from pyproj import CRS
import pandas as pd
from datetime import datetime, date, timedelta
from ServiceDB import DBsql
from Table1_sector import Sector_grid
from Table2_region import ServiceProcessing
from Table3_status import DriverServiceProcessing
from TableDBUpdate import DBupdate
from kakaoapi import KakaoLocalAPI
from tqdm import tqdm

# 2. 라이브 데이터 조회
## Live service 이용내역 데이터 불러오기
print('select_live_data')
obj = DBsql()
# service_data 함수로 서비스 이용내역 데이터 불러오기(호출 + 일반 주행)
service = obj.service_data()
#ServiceDB 연결 해제
del obj

# service.to_excel('service.xlsx', encoding='utf-8-sig',index = False)

## drirver_work_log 함수로 기사 로그 데이터 불러오기
obj = DBsql()
driver_work_log = obj.driver_work_log()
#ServiceDB 연결 해제
del obj

## service data object로 되어 있는 위경도 타입 변경 -> SQL에서 타입 변경이 되지 않는 컬럼
service['출발지 위도'] = service['출발지 위도'].astype(float)
service['출발지 경도'] = service['출발지 경도'].astype(float)
service['도착지 위도'] = service['도착지 위도'].astype(float)
service['도착지 경도'] = service['도착지 경도'].astype(float)

# print(driver_work_log.info())

##driver_work_log WGS84 좌표 타입 변환
# driver_work_log['lat'] = driver_work_log['lat'].astype(float)
# driver_work_log['lng'] = driver_work_log['lng'].astype(float)

# 3. 데이터 가공
## 1) Table1 : sector 판별
print('wgstokatec')
### 위경도 좌표 katec으로 변환
WGS84 = {'proj':'latlong', 'datum':'WGS84', 'ellps':'WGS84'}
TM128 = { 'proj':'tmerc', 'lat_0':'38N', 'lon_0':'128E', 'ellps':'bessel',
   'x_0':'400000', 'y_0':'600000', 'k':'0.9999','towgs84':'-146.43,507.89,681.46'}
transformer = Transformer.from_crs(CRS(**WGS84),CRS(**TM128))
s_converted=transformer.transform(service['출발지 경도'].values, service['출발지 위도'].values)
g_converted=transformer.transform(service['도착지 경도'].values, service['도착지 위도'].values)

### service data에 출발지, 도착지 카텍 좌표 붙여넣기
service['start_xpos'] = s_converted[0]
service['start_ypos'] = s_converted[1]
service['goal_xpos'] = g_converted[0]
service['goal_ypos'] = g_converted[1]

### Table1_sector.py 에 있는 Sector_grid() 함수 실행
sector = Sector_grid()
### control_section_data 불러오기 (배차 Dev 서버)
sector.get_sector_data()

print('define_s_sector_start')
### 출발지 섹터 리스트 만들기
s_sector_list = []
for i in tqdm(range(len(service))):
   xpos = service['start_xpos'][i]
   ypos = service['start_ypos'][i]
   s_sector_list.append(sector.get_sector(xpos, ypos))

print('define_g_sector_start')

### 도착지 섹터 리스트 만들기 변경
g_sector_list = []
for i in tqdm(range(len(service))):
   xpos = service['goal_xpos'][i]
   ypos = service['goal_ypos'][i]
   g_sector_list.append(sector.get_sector(xpos, ypos))

print('define_sector_finish')

### service data에 출발지 섹터와 도착지 섹터 컬럼 추가
service['section'] = s_sector_list
service['gsection'] = g_sector_list
### 출발지 섹터와 도착지 섹터의 null 값 9999로 대체 후 int 타입으로 변경
service['section'] = service['section'].fillna(9999)
service['section'] = service['section'].astype('int64')
service['gsection'] = service['gsection'].fillna(9999)
service['gsection'] = service['gsection'].astype('int64')


## 2) Table_2 가공 (주소, 주행시간, 배차시간, 주행거리 가공)
### 콜센터 주소 보정
kakao = KakaoLocalAPI()

call_address = service[service['구분'] == '콜센터']
call_address = call_address.reset_index().drop('index', axis=1)

s_address_list = []
g_address_list = []

print('modify_address')

for i in range(len(call_address)):
   s_x = call_address['출발지 경도'][i]
   s_y = call_address['출발지 위도'][i]
   g_x = call_address['도착지 경도'][i]
   g_y = call_address['도착지 위도'][i]

   #출발지 주소
   s_result = kakao.geo_coord2address(s_x,s_y,'WGS84')
   if s_result['documents'][0]['address'] == None:
      s_address = s_result['documents'][0]['road_address']['address_name']
      s_address_list.append(s_address)

   else:
      s_address = s_result['documents'][0]['address']['address_name']
      s_address_list.append(s_address)

   #도착지 주소
   g_result = kakao.geo_coord2address(g_x,g_y,'WGS84')
   if g_result['documents'][0]['address'] == None:
      g_address = g_result['documents'][0]['road_address']['address_name']
      g_address_list.append(g_address)

   else:
      g_address = g_result['documents'][0]['address']['address_name']
      g_address_list.append(g_address)

service.loc[service['구분'] == '콜센터','출발지 상세'] = s_address_list
service.loc[service['구분'] == '콜센터','도착지 상세'] = g_address_list
print('finish modify address')

### 시간 및 요금 가공
print('time and fare processing')
#2월 8일 추가 '이용일시' date_time으로 변경
# service['이용일시'] = pd.to_datetime(service['이용일시'])
service['일'] = service['이용일시'].dt.day
service['시간'] = service['이용일시'].dt.hour
service['요일'] = service['이용일시'].dt.weekday
service['요금합계'] = service['요금'] + service['통행료'] + service['추가금액'] - service['할인금액'] - service['사용 포인트']
print('time and fare finish')

### 행정구역 나누기(도,시,구,동)

print('region_process_start')
process = ServiceProcessing(service)
process.start_address()
process.goal_address()
process.trip_time()
process.dispatch_time()

service = process.service

## 3) Table 3 가공 (배차거리, 상태변경까지 걸린시간, 상태 변경까지 걸린 거리)

### service 컬럼 추가
service['배차시 승객과의 거리(m)'] = 0
service['상태변경까지 걸린시간(s)'] = 0
service['상태변경까지 걸린거리(m)'] = 0

### service 상태별 가공
print('driver_process start')
driver_process = DriverServiceProcessing(service, driver_work_log)
driver_process.pickoff_status()
driver_process.user_cancel()
driver_process.driver_cancel()
driver_process.g_b_cancel()
driver_process.race_stop()
driver_process.no_show()
print('driver_process finish')

service = driver_process.service

# 4. 분석용 DB (TableDBUPdate)에 업데이트
dbu = DBupdate()
## Table_1 에 업데이트
print('im_service_section update start')
dbu.table1_update(service)
del dbu
print('im_service_section update finish')

## Table2업데이트
print('im_service_car_log update start')
dbu = DBupdate()
dbu.table2_update(service)
del dbu
print('im_service_car_log update finish')

## Table 3 업데이트
dbu = DBupdate()
print('im_service_driver_log update start')
dbu.table3_update(service)
del dbu
print('im_service_driver_log update finish')
print('program finish')




