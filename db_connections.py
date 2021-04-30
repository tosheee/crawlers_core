import pymysql
import json
from dbconfig import mysql_prop

class DbConnection(object):

    def select_identifier(self, identifier):
        self.conn = pymysql.connect(**mysql_prop)
        self.cursor = self.conn.cursor()

        sql = "SELECT id, identifier, content FROM `organizations` WHERE `identifier`=%s"
        self.cursor.execute(sql, (identifier,))
        result = self.cursor.fetchone()
        content = json.loads(result[2])
        # import pdb;
        # pdb.set_trace()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
