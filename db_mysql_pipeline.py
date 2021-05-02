from mysql_connection import MysqlConnection
import json
from datetime import datetime


class DbMysqlPipeline(MysqlConnection):

    def process_item(self, item, spider):
        status = self.check_item_status(item)
        item['content']['status'] = status

        if status == 'No changed':
            print("\nTHE ARTICLE IS OLD AND NOT UPDATED\n")
            return item

        if status == 'Updated offer':
            print("\nTHE ARTICLE WILL BE UPDATED\n")
            self.update_item(item)
            return item

        if status == 'New offer':
            print("\nTHE ARTICLE IS NEW\n")
            self.insert_item(item)

        return item

    def check_item_status(self, item):

        old_item = self.select_item(item)

        if not old_item:
            return 'New offer'

        old_item_content = json.loads(old_item[2]).get('content', {})
        updated_fields = []

        for key, val in dict(item).get('content').items():
            if key == 'current_url':
                continue

            if old_item_content.get(key, '') != val:
                print(f'THE FIELD {key} is UPDATED')
                print(f'Value: {val}')
                updated_fields.append({key: val})

        return 'Updated offer' if updated_fields else 'No changed'

    def select_item(self, item):
        article_id = item.get('content', {}).get('article_id', '')
        select_sql = "SELECT * FROM `items` WHERE `article_id`=%s"
        self.cursor.execute(select_sql, (article_id,))

        return self.cursor.fetchone()

    def update_item(self, item):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

        article_id = item.get('content', {}).get('article_id')
        update_sql = "UPDATE `items` SET `item_data`=%s, `updated_at`=%s WHERE `article_id`=%s"
        item_data = json.dumps(dict(item))
        self.cursor.execute(update_sql, (item_data, timestamp, article_id))
        self.conn.commit()

        pass

    def insert_item(self, item):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

        article_id = item.get('content', {}).get('article_id', '')
        insert_sql = """insert into items (article_id, item_data, created_at, updated_at) VALUES (%s, %s, %s, %s)"""
        article_data = json.dumps(dict(item))
        self.cursor.execute(insert_sql, (article_id, article_data, timestamp, timestamp))
        self.conn.commit()

        pass
