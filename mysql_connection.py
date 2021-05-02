import pymysql
from dbconfig import mysql_prop

class MysqlConnection(object):
    def __init__(self):
        self.conn = pymysql.connect(**mysql_prop)
        self.cursor = self.conn.cursor()

    def close_spider(self):
        self.cursor.close()
        self.conn.close()
