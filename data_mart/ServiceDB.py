import pandas as pd
import pymysql

class DBsql:
    def __init__(self):
 
        host = '*****'
        username = '******'
        pw = '*****'
        port = '*****'
        # db = '******'
        db = '******'


        self.conn = pymysql.connect(host=host, user=username, db=db, port=port, password=pw)
        # self.conn.cursor(pymysql.cursors.DictCursor)


    def service_data(self):
        # curs = self.conn.cursor(pymysql.cursors.DictCursor)
        # curs.execute(query)
        # data = pd.DataFrame(curs.fetchall())
        conn = self.conn
        query = """
    SELECT a.reg_datetime as '이용일시',
          a.dispatch_idx as '배차 ID',
          NULL AS 'general_boarding_uuid',
       	  (case a.boarding_type 
	       when '1' then '개인'
               when '2' then '비즈니스'
               when '3' then '아이맘' END) AS '서비스',
      
         (case when  (a.is_reservation = 'Y' ) THEN
                 case when (a.dispatch_status = 1) THEN '예약호출' END
                
                when (a.dispatch_status = 1) THEN '앱호출' ELSE '콜센터'
              
         END) AS '구분',
      d.dispatch_type as 'dispatch_type',
    
	 (case a.`status`
		when '1' then '호출'
		when '2' then '호출취소'
		when '3' then '배차'
		when '4' then '배차거절'
		when '5' then '배차실패'
		when '6' then '배차취소-사용자'
		when '7' then '배차취소-기사'
		when '8' then '도착'
		when '9' then '탑승'
		when '10' then '출발'
		when '11' then '미탑승신고'
		when '12' then '주행중지'
		when '13' then '하차' END) AS '상태',
			
	b.company_idx AS company_idx,
      
       (case b.company_idx
            when '1' then '동부교통'
            when '2' then '동부기업'
            when '3' then '동부상운'
            when '4' then '동부실업'
            when '5' then '동부운수'
            when '6' then '동부택시'
            when '7' then '대영운수'
            when '8' then '금강상운'
            when '9' then '서연교통'
            when '10' then '제이엠투'
            when '11' then '제이엠쓰리'
            when '12' then '제이엠포'
            when '9999' then '엠에이치큐'END) AS '택시회사', 
      
       a.driver_idx AS '기사 일련번호',
       b.car_number AS '차량번호',
       d.car_type_idx AS 'car_type',
       c.`name` AS '고객명',
       c.phone_number AS '고객 전화번호',
       a.departure_lat  AS '출발지 위도',
       a.departure_lng AS '출발지 경도',
       a.departure_address AS '출발지',
       a.departure_address_detail AS '출발지 상세',
       a.departure_datetime AS '출발일시',
       a.arrival_lat AS '도착지 위도',
       a.arrival_lng AS '도착지 경도',
       a.arrival_address AS '도착지',
       a.arrival_address_detail AS '도착지 상세',
       a.arrival_datetime AS '도착일시',
       a.estimated_amount AS '예상금액',
       0 AS '대절요금',
       
	   (case a.payment_type
		      when '1' then '직접결제'
		      when '2' then '자동결제'
				ELSE '후불결제' END) AS '결제방법',
		
		 ifnull(cast(a.fare AS SIGNED),0) AS '요금',
		 0 AS '취소수수료',
		 ifnull(cast(a.toll AS SIGNED),0) AS '통행료',
		 ifnull(cast(a.additional_amount AS SIGNED),0) AS '추가금액',
		 ifnull(cast(a.discount_amount AS SIGNED),0) AS '할인금액',
		 ifnull(cast(a.use_point AS SIGNED),0) AS '사용 포인트',
		 ifnull(cast(a.driving_distance AS SIGNED),0) AS '주행거리',
		 IFNULL(CAST(a.estimated_distance AS SIGNED),0) AS '예상거리',
		 a.is_call AS 'is_call'
		 
         
FROM im_mobility.boarding_history a
LEFT JOIN im_mobility.car b ON a.car_idx = b.car_idx JOIN im_mobility.user c ON a.user_idx = c.user_idx
     JOIN im_mobility.boarding_history_additional d on a.idx = d.boarding_history_idx 
WHERE a.reg_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')

UNION ALL

SELECT   a.reservation_datetime AS '이용일시',
          0 AS '배차 ID',
          NULL AS 'general_boarding_uuid',
         '개인' AS '서비스',
         '예약호출' AS '구분',
         1 as 'dispatch_type',
         '예약취소' AS '상태',
         NULL AS 'company_idx',
         NULL AS '택시회사',
         a.driver_idx AS '기사 일련번호',
         NULL AS '차량번호',
         a.car_type_idx AS 'car_type',
         b.`name` AS '고객명',
         b.phone_number AS '고객 전화번호',
         a.departure_lat AS '출발지 위도',
         a.departure_lng AS '출발지 경도',
         a.departure_address AS '출발지',
         a.departure_address_detail AS '출발지 상세',
         NULL AS '출발일시',
         a.arrival_lat '도착지 위도',
         a.arrival_lng '도착지 경도',
         a.arrival_address '도착지',
         a.arrival_address_detail '도착지 상세',
         NULL AS '도착일시',
         0 AS '예상금액',
         ifnull(CAST(a.fare AS SIGNED),0) AS '대절요금',
         '자동결제' AS '결제방법',
         0 AS '요금',
         ifnull(CAST(a.fee_fare AS SIGNED),0) AS '취소수수료',
         0 AS '통행료',
         0 AS '추가금액',
         ifnull(CAST(a.discount_amount AS SIGNED),0) AS '할인금액',
         ifnull(CAST(a.use_point AS SIGNED),0) AS '사용 포인트',
         0 AS '주행거리',
         0 AS '예상거리',
         a.is_call AS 'is_call'
            
FROM im_mobility.reservation_boarding_history a LEFT JOIN im_mobility.user b on a.user_idx = b.user_idx
WHERE a.is_cancel = 'Y' AND a.reservation_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')

UNION ALL

SELECT a.reg_datetime AS '이용일시',
       0 AS '배차 ID',
       NULL AS 'general_boarding_uuid',
       '개인' AS '서비스',
       '일반주행' AS '구분',
       1 AS 'dispatch_type',
       (case a.`status` 
		       when '1' then '주행'
				 when '2' then '하차'
				 when '3' then '취소' END)AS '상태',
		
		 b.company_idx AS company_idx,
			
		 (case b.company_idx
            when '1' then '동부교통'
            when '2' then '동부기업'
            when '3' then '동부상운'
            when '4' then '동부실업'
            when '5' then '동부운수'
            when '6' then '동부택시'
            when '7' then '대영운수'
            when '8' then '금강상운'
            when '9' then '서연교통'
            when '10' then '제이엠투'
            when '11' then '제이엠쓰리'
            when '12' then '제이엠포'
            when '9999' then '엠에이치큐'END) AS '택시회사',
      
       a.driver_idx AS '기사 일련번호',
       b.car_number AS '차량번호',
       1 AS 'car_type',
       NULL AS '고객명',
       NULL AS '고객 전화번호',
       a.departure_lat AS '출발지 위도',
       a.departure_lng AS '출발지 경도',
       a.departure_address AS '출발지',
       a.departure_address_detail AS '출발지 상세',
       a.departure_datetime AS '출발일시',
       a.arrival_lat AS '도착지 위도',
       a.arrival_lng AS '도착지 경도',
       a.arrival_address AS '도착지',
       a.arrival_address_detail AS '도착지 상세',
       a.arrival_datetime AS '도착일시',
       0 AS '예상금액' ,
       0 AS '대절요금' ,
       '직접결제' AS '결제방법',
       ifnull(cast(a.fare AS SIGNED),0) AS '요금',
       0 AS '취소수수료',
		 ifnull(cast(a.toll AS SIGNED),0) AS '통행료',
		 ifnull(cast(a.additional_amount AS SIGNED),0) AS '추가금액',
		 0 AS '할인금액',
		 0 AS '사용 포인트',
		 ifnull(cast(a.driving_distance AS SIGNED) ,0) AS '주행거리',
		 0 AS '예상거리',
		 'N' AS 'is_call'
		 
FROM im_mobility.general_boarding_history a
LEFT JOIN im_mobility.car b ON a.car_idx = b.car_idx

WHERE a.reg_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')
ORDER BY 이용일시;
        """
        data = pd.read_sql(query, conn)

        return data

    def driver_work_log(self):
        conn = self.conn
        query = ''' SELECT a.log_idx,
                       a.driver_work_history_idx,
                       a.company_idx,
                       a.driver_idx,
                       a.driver_uuid,
                       a.work_day,
                       CAST(a.lat AS DECIMAL(10,7)) AS lat,
                       CAST(a.lng AS DECIMAL(10,7)) AS lng,
                       a.`status`,
                       a.dispatch_idx,
                       a.general_boarding_uuid,
                       a.reg_utime,
                       a.reg_datetime
                    FROM im_mobility.driver_work_log a
                    WHERE  a.reg_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND NOW() AND
                            a.status <> 30'''
        data = pd.read_sql(query,conn)

        return data


    def __del__(self):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()