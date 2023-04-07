import pandas as pd
import numpy as np

import pymysql


class Sector_grid:
    def __init__(self):
        # # 배차 Dev server 접근
        host = '*******'
        username = '*******'
        password = '********'
        port = '*******'
        database = '*******'

        self.conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)

        self.conn.cursor(pymysql.cursors.DictCursor)

    # def __del__(self):
    #     curs = self.conn.cursor(pymysql.cursors.DictCursor)
    #     curs.close()
    #     self.conn.close()

    def get_sector_data(self):
        conn = self.conn
        query = """SELECT control_section_data.left,
                          control_section_data.right,
                          control_section_data.top,
                          control_section_data.bottom,
                          control_section_data.section
                          FROM `pos`.`control_section_data` """

        self.data = pd.read_sql(query, conn)

        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()

    # grid 내 있는지 판별
    def get_grid(self, xpos, ypos):

        self.data['left'] = self.data['left'].astype(float)
        self.data['right'] = self.data['right'].astype(float)
        self.data['top'] = self.data['top'].astype(float)
        self.data['bottom'] = self.data['bottom'].astype(float)

        left_min = self.data['left'].min()
        right_max = self.data['right'].max()
        bottom_min = self.data['bottom'].min()
        top_max = self.data['top'].max()

        if ((xpos >= left_min) & (xpos <= right_max)) & ((ypos >= bottom_min) & (ypos <= top_max)):
            answer = 1

        else:
            answer = 0

        return answer

    # 어느 섹터 인지 판멸
    def get_sector(self, xpos, ypos):

        if self.get_grid(xpos, ypos) == 1:

            for section in np.arange(0, 3800, 100):

                left_x = self.data[self.data['section'] == section]['left'].values[0]
                right_x = self.data[self.data['section'] == section]['right'].values[0]

                if (xpos >= left_x) & (xpos < right_x):

                    for y in np.arange(0, 31, 1):

                        bottom_y = self.data[self.data['section'] == section + y]['bottom'].values[0]
                        top_y = self.data[self.data['section'] == section + y]['top'].values[0]

                        if (ypos >= bottom_y) & (ypos < top_y):
                            sector = section + y

                            return sector



        elif self.get_grid(xpos, ypos) == 0:
            sector = 9999

            return sector

