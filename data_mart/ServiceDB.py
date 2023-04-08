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
    SELECT a.reg_datetime 
          a.dispatch_idx 
          NULL AS '****',
       	  (case a.***** 
	       when '1' then '**'
               when '2' then '**'
               when '3' then '**' END) AS '서비스',
      
         (case when  (a.** = 'Y' ) THEN
                 case when (a.** = 1) THEN '**' END
                
                when (a.********* = 1) THEN '****' ELSE '****'
              
         END) AS '**',
      d.****,
    
	 (case a.`***`
		when '1' then '**'
		when '2' then '**'
		when '3' then '**'
		when '4' then '**'
		when '5' then '**'
		when '6' then '**'
		when '7' then '**'
		when '8' then '**'
		when '9' then '**'
		when '10' then '**'
		when '11' then '**'
		when '12' then '**'
		when '13' then '**' END) AS '**',
			
	b.**,
      
       (case b.**
            when '1' then '**'
            when '2' then '**'
            when '3' then '**'
            when '4' then '**'
            when '5' then '**'
            when '6' then '**'
            when '7' then '**'
            when '8' then '**'
            when '9' then '**'
            when '10' then '**'
            when '11' then '**'
            when '12' then '**'
            when '9999' then '**'END) AS '**', 
      
       a.**,
       b.**,
       d.**,
       c.**,
       c.**,
       a.**,
       a.**,
       a.**',
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       0 AS '**',
       
	   (case a.**
		      when '1' then '**'
		      when '2' then '**'
				ELSE '**' END) AS '**',
		
		 ifnull(cast(a.fare AS SIGNED),0) AS '요금',
		 0 AS '취소수수료',
		 ifnull(cast(a.toll AS SIGNED),0) AS '통행료',
		 ifnull(cast(a.additional_amount AS SIGNED),0) AS '추가금액',
		 ifnull(cast(a.discount_amount AS SIGNED),0) AS '할인금액',
		 ifnull(cast(a.use_point AS SIGNED),0) AS '사용 포인트',
		 ifnull(cast(a.driving_distance AS SIGNED),0) AS '주행거리',
		 IFNULL(CAST(a.estimated_distance AS SIGNED),0) AS '예상거리',
		 a.is_call AS 'is_call'
		 
         
FROM im_mobility.table a
LEFT JOIN im_mobility.nn b ON a.** = b.** JOIN im_mobility.** c ON a.** = c.**
     JOIN im_mobility.mm d on a.** = d.** 
WHERE a.reg_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')

UNION ALL

SELECT   a.** AS,
          0 AS '배차 ID',
          NULL AS '**',
         '**' AS '**',
         '**' AS '**',
         1 as '**',
         '**' AS '**',
         NULL AS '**',
         NULL AS '**',
         a.**,
         NULL AS '**',
         a.** AS '**',
         b.** AS '**',
         b.**,
         a.**,
         a.**',
         a.**,
         a.**,
         NULL AS '**',
         a.**,
         a.**,
         a.**,
         a.**,
         NULL AS '**',
         0 AS '**',
         ifnull(CAST(a.fare AS SIGNED),0),
         '**' AS '**',
         0 AS '요금',
         ifnull(CAST(a.fee_fare AS SIGNED),0) AS '취소수수료',
         0 AS '통행료',
         0 AS '추가금액',
         ifnull(CAST(a.discount_amount AS SIGNED),0) AS '할인금액',
         ifnull(CAST(a.use_point AS SIGNED),0) AS '사용 포인트',
         0 AS '주행거리',
         0 AS '예상거리',
         a.is_call AS 'is_call'
            
FROM im_mobility.table a LEFT JOIN im_mobility.nn b on a.** = b.**
WHERE a.** = 'Y' AND a.** BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')

UNION ALL

SELECT a.reg_datetime,
       0 AS '**',
       NULL AS '**',
       '**' AS '**',
       '**' AS '**',
       1 AS '**',
       (case a.`**` 
		       when '1' then '**'
				 when '2' then '**'
				 when '3' then '**' END)AS '**',
		
		 b.** ,
			
		 (case b.**
            when '1' then '**'
            when '2' then '**'
            when '3' then '**'
            when '4' then '**'
            when '5' then '**'
            when '6' then '**'
            when '7' then '**'
            when '8' then '**'
            when '9' then '**'
            when '10' then '**'
            when '11' then '**'
            when '12' then '**'
            when '9999' then '**'END) AS '**',
      
       a.**,
       b.**,
       1 AS '**',
       NULL AS '**',
       NULL AS '**',
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       a.**,
       0 AS '**' ,
       0 AS '**' ,
       '**' AS '**',
       ifnull(cast(a.fare AS SIGNED),0) AS '요금',
       0 AS '취소수수료',
		 ifnull(cast(a.toll AS SIGNED),0) AS '통행료',
		 ifnull(cast(a.additional_amount AS SIGNED),0) AS '추가금액',
		 0 AS '할인금액',
		 0 AS '사용 포인트',
		 ifnull(cast(a.driving_distance AS SIGNED) ,0) AS '주행거리',
		 0 AS '예상거리',
		 'N' AS 'is_call'
		 
FROM im_mobility.table a
LEFT JOIN im_mobility.nn b ON a.** = b.**

WHERE a.reg_datetime BETWEEN DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1 DAY), '%Y-%m-%d 06:00:00') AND DATE_FORMAT(NOW(), '%Y-%m-%d 05:59:59')
ORDER BY 이용일시;
        """
        data = pd.read_sql(query, conn)

        return data

    def driver_work_log(self):
        conn = self.conn
        query = ''' SELECT a.log_idx,
                       a.**,
                       a.**,
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