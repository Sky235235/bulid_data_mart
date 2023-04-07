import pandas as pd
import numpy as np

from urllib.parse import urlencode, quote_plus
import requests
import json




class WeatherAPI:
    '''
    Weather API 컨트롤러
    '''

    def __init__(self):
        '''
        Rest API 키 초기화 및 기능 별 URL 설정
        '''
        service_key = '*'
        # service API 키 설정
        self.service_key = service_key

        #URL 설정
        self.URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

    def get_weather(self,day):

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): self.service_key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '700',
                                       quote_plus('dataType'): 'JSON',
                                       quote_plus('dataCd'): 'ASOS',
                                       quote_plus('dateCd'): 'HR',
                                       quote_plus('startDt'): day,
                                       quote_plus('startHh'): '00',
                                       quote_plus('endDt'): day,
                                       quote_plus('endHh'): '23',
                                       quote_plus('stnIds'): '108'})

        res = requests.get(self.URL + queryParams)
        document = json.loads(res.text)
        weather = pd.json_normalize(document['response']['body']['items']['item'])
        col = ['tm', 'stnId', 'stnNm', 'ta', 'rn', 'ws', 'hm']
        weather = weather[col]
        weather.columns = ['일시', '지점', '지점명', '기온', '강수량', '풍속', '습도']

        return weather



