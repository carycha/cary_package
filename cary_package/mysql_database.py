import os

import pymysql

# from pymysql.cursors import DictCursor
import json
import time
import ndjson
import pandas as pd


class mysql_database:
    # 預設資料庫類型為MySQL，如果要改用其它資料庫，secret.json要記得加連線資訊
    def __init__(self, secret_dict, db_name):
        self.secret_dict = secret_dict
        self.db_name = db_name

    def get_connect(self):
        # 再拿 skyurl mysql 的值，之後會用
        # secret = get_secret_dict(secret_name)
        secret_dict = self.secret_dict

        host = secret_dict["host"]
        # port = secret_dict["port"]
        port = secret_dict.get("port", "")
        user_name = secret_dict["user_name"]
        user_password = secret_dict["user_password"]

        mysql_connect = object
        try:
            if port:
                mysql_connect = pymysql.connect(
                    host=host,
                    port=int(port),
                    user=user_name,
                    password=user_password,
                    # db=db_name,
                    charset="utf8mb4",
                )
            else:
                mysql_connect = pymysql.connect(
                    host=host,
                    # port=int(port),
                    user=user_name,
                    password=user_password,
                    # db=db_name,
                    charset="utf8mb4",
                )
            # print("MySQL資料庫連接完成")
        except Exception as e:
            raise Exception("MySQL資料庫連接失敗：", e)
        return mysql_connect

    # 中斷DB連線
    def disconnect(self, conn):
        returnData = {}
        try:
            conn.close()
            returnData = {"status": "success", "message": "Connection closed."}
        except Exception as e:
            returnData = {"status": "failure", "message": e}

        return returnData

    # 取得指標

    def get_cursor(self, conn):
        cursor = pymysql.cursors.SSDictCursor(conn)
        return cursor

    def create_mysql_database(self):
        db_name = self.db_name
        sql = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
        try:
            self.execute(sql)
            returnData = {"status": "success", "message": "Query done."}
        except Exception as e:
            returnData = {"status": "failure", "message": e}
        print(returnData)
        return returnData

    def check_table_exist(self, db_name, table_name):
        sql = """
            SHOW TABLES from {} like '{}'
        """.format(
            db_name, table_name
        )
        result = self.execute(sql)
        if result:
            return True
        return False

    def execute(self, sql):
        # to do 看要不要改寫成判斷不存在就新增，存在就重建
        return_list = []
        conn = self.get_connect()
        cursor = self.get_cursor(conn)
        try:
            cursor.execute(sql)
            for result in cursor:
                return_list.append(result)
            # result = cursor.fetchall()
            conn.commit()
        except Exception as e:
            cursor.close()
            self.disconnect(conn)
            if "lock" in str(e).lower():
                print("deadlock!睡一下再來一次")
                time.sleep(60)
                self.execute(sql)
            else:
                raise Exception("execute失敗:{}".format(e))
        cursor.close()
        self.disconnect(conn)

        return return_list

    def executemany(self, sql, data):
        conn = self.get_connect()
        cursor = self.get_cursor(conn)
        try:
            cursor.executemany(sql, data)
            conn.commit()
            print("結束executemany 資料量:{}".format(len(data)))
        except Exception as e:
            cursor.close()
            self.disconnect(conn)
            if "lock" in str(e).lower():
                print("deadlock!睡一下再來一次")
                time.sleep(10)
                self.executemany(sql, data)
            else:
                with open("./error_log.ndjson", "a+") as f:
                    ndjson.dump(data, f, ensure_ascii=False)
                    f.write("\n")
                raise Exception("executemany失敗:{}".format(e))
        cursor.close()
        self.disconnect(conn)
        return

    def convert_dict_item_to_str(self, list_dict):
        # 因為發生insert因缺少欄位錯誤，改變使用dataframe讓少的欄位變成''
        df = pd.DataFrame(list_dict).fillna("")
        list_dict = df.to_dict("records")
        # 寫入mysql時需要轉成str，但最後要記得把'轉成"，不然以後query出來時會變字串還要另外轉很煩..
        # 嘗試改成jsondumps 看看效果如何
        for row in list_dict:
            for key in row:
                # 把' 改成", 這樣以後query出來就不用再轉dict了..
                if dict == type(row[key]) or list == type(row[key]):
                    row[key] = json.dumps(row[key], ensure_ascii=False)
                else:
                    # row[key] = str(row[key]).replace("'", '"')
                    reformat_str = str(row[key])
                    # 避免回傳資料是null，直接轉為""
                    if "null" == reformat_str.lower():
                        reformat_str = ""
                    row[key] = reformat_str
        return list_dict

    def split_list_smaller(self, arr, size):
        arrs = []
        while len(arr) > size:
            pice = arr[:size]
            arrs.append(pice)
            arr = arr[size:]
        arrs.append(arr)
        return arrs

    def get_insert_sql(self, target_table_name, keys, values):
        db_name = self.db_name
        insert_query = "INSERT INTO {}.{} ({}) VALUES ({})".format(
            db_name, target_table_name, keys, values
        )
        return insert_query

    def get_insert_update_sql(self, target_table_name, keys, values):
        key_list = keys.split(",")
        # value_list = values.split(",")
        update_list = []
        for i, key in enumerate(key_list):
            temp_str = "{}=values({})".format(key, key)
            update_list.append(temp_str)
        update_str = ",".join(update_list)

        db_name = self.db_name
        insert_query = (
            "INSERT INTO {}.{} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {};".format(
                db_name, target_table_name, keys, values, update_str
            )
        )
        return insert_query

    def get_replace_into_sql(self, target_table_name, keys, values):
        db_name = self.db_name
        insert_query = "REPLACE INTO {}.{} ({}) VALUES ({})".format(
            db_name, target_table_name, keys, values
        )
        return insert_query

    def generate_import_mysql_columes_values_list(self, import_data):
        keys = ",".join(import_data[0].keys())
        values_list = []
        for i in import_data[0].keys():
            values_list.append("%({})s".format(i))
        values = ",".join(values_list)
        return keys, values


if __name__ == "__main__":
    print("Call it locally")