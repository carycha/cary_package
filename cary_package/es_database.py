import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
from elasticsearch import AsyncElasticsearch


class es_database:
    def __init__(self, secret_dict, index_name):
        self.secret_dict = secret_dict
        self.index_name = index_name
        # self.settings = settings
        # self.mappings = mapping
        # self.aliases = aliases

    # def es_get_connect_bk(self):
    #     secret_dict = self.secret_dict
    #     host = secret_dict["host"]
    #     port = secret_dict.get("port", "")
    #     http = secret_dict.get("http", "http")
    #     es_hosts = ["{}:{}".format(host, port)]
    #     es_auth = (secret_dict["user_name"], secret_dict["user_password"])
    #     # 現在是http，若變成線上的https要記得改
    #     try:
    #         es = Elasticsearch(es_hosts, http_auth=es_auth, scheme=http, timeout=60)
    #     except Exception as e:
    #         raise Exception("es資料庫連接失敗：", e)
    #     return es
    def es_get_connect_async(self):
        config = self.secret_dict
        host = config["ES_HOST"]
        es_auth = (config["ES_USER"], config["ES_PASSWORD"])
        try:
            es = AsyncElasticsearch(
                # url,
                host.split(","),
                http_auth=es_auth,
                # scheme=http_type,
                # port=port,
                timeout=60,
                # http_compress=True,
            )
        except Exception as e:
            raise Exception("es資料庫連接失敗：", e)
        return es
        # secret_dict = self.secret_dict
        # host = secret_dict["host"]
        # port = secret_dict.get("port", "")
        # http_type = secret_dict.get("http_type", "http")
        # sub_path = secret_dict.get("sub_path", "")
        # if "https" == http_type and not port:
        #     port = 443
        # # es_hosts = [host]

        # if sub_path:
        #     url = "{}://{}:{}/{}".format(http_type, host, port, sub_path)
        # else:
        #     url = "{}://{}:{}".format(http_type, host, port)
        # es_auth = (secret_dict["user_name"], secret_dict["user_password"])
        # try:
        #     es = AsyncElasticsearch(
        #         # es = Elasticsearch(
        #         url,
        #         http_auth=es_auth,
        #         # scheme=http_type,
        #         # port=port,
        #         timeout=60,
        #     )
        # except Exception as e:
        #     raise Exception("es資料庫連接失敗：", e)
        # return es

    def es_get_connect(self):
        config = self.secret_dict
        host = config["ES_HOST"]
        es_auth = (config["ES_USER"], config["ES_PASSWORD"])
        try:
            es = Elasticsearch(
                # url,
                host.split(","),
                http_auth=es_auth,
                # scheme=http_type,
                # port=port,
                timeout=60,
            )
        except Exception as e:
            raise Exception("es資料庫連接失敗：", e)
        return es

        # host = secret_dict["host"]
        # port = secret_dict.get("port", "")
        # http_type = secret_dict.get("http_type", "http")
        # sub_path = secret_dict.get("sub_path", "")
        # if "https" == http_type and not port:
        #     port = 443
        # # es_hosts = [host]
        # if sub_path:
        #     url = "{}://{}:{}/{}".format(http_type, host, port, sub_path)
        # else:
        #     url = "{}://{}:{}".format(http_type, host, port)
        # es_auth = (secret_dict["user_name"], secret_dict["user_password"])

        # try:
        #     # es = AsyncElasticsearch(
        #     es = Elasticsearch(
        #         url,
        #         http_auth=es_auth,
        #         # scheme=http_type,
        #         # port=port,
        #         timeout=60,
        #     )
        # except Exception as e:
        #     raise Exception("es資料庫連接失敗：", e)
        # return es

    def es_bulk_insert_doc_into_index(self, resource_list):
        es = self.es_get_connect()
        index_name = self.index_name
        # 塞入資料
        info = helpers.bulk(
            es,
            resource_list,
            index=index_name,
            # chunk_size=300,
            chunk_size=400,
        )
        # print(info)
        return info

    async def es_async_bulk_insert_doc_into_index(self, resource_list):
        es = self.es_get_connect_async()
        index_name = self.index_name
        # 塞入資料
        info = await helpers.async_bulk(
            es,
            resource_list,
            index=index_name,
            # chunk_size=300,
            # chunk_size=400,
            chunk_size=400,
            # http_compress=True,
        )
        # print(info)
        await es.close()
        return info

    def es_bulk_delete_doc_by_id(self, id_list):
        es = self.es_get_connect()
        index_name = self.index_name
        action_list = []
        for row in id_list:
            temp_dict = {}
            temp_dict["_op_type"] = "delete"
            temp_dict["_id"] = row

            action_list.append(temp_dict)
        # 塞入資料
        helpers.bulk(es, action_list, index=index_name, chunk_size=400, ignore_status=[404])
        return

    def es_bulk_update_upsert_doc_by_id(self, data_list):
        es = self.es_get_connect()
        index_name = self.index_name
        action_list = []
        for row in data_list:
            temp_dict = {}
            temp_dict["_op_type"] = "update"
            # temp_dict["_op_type"] = "index"
            temp_dict["doc_as_upsert"] = True
            temp_dict["_id"] = row["_id"]
            row.pop("_id")
            temp_dict["doc"] = row

            action_list.append(temp_dict)

        # 塞入資料
        result = helpers.bulk(
            es, action_list, index=index_name, chunk_size=400  # , ignore_status=[404]
        )
        return result

    async def es_async_bulk_update_upsert_doc_by_id(self, data_list):
        es = self.es_get_connect_async()
        index_name = self.index_name
        action_list = []
        for row in data_list:
            temp_dict = {}
            temp_dict["_op_type"] = "update"
            # temp_dict["_op_type"] = "index"
            temp_dict["doc_as_upsert"] = True
            temp_dict["_id"] = row["_id"]
            row.pop("_id")
            temp_dict["doc"] = row

            action_list.append(temp_dict)

        # 塞入資料
        # result = helpers.bulk(
        result = await helpers.async_bulk(
            es,
            action_list,
            index=index_name,
            chunk_size=400,  # , ignore_status=[404]
        )
        # print(result)
        await es.close()
        return result

    def es_bulk_index_doc_by_id(self, data_list):
        es = self.es_get_connect()
        index_name = self.index_name
        action_list = []
        for row in data_list:
            temp_dict = {}
            temp_dict["_op_type"] = "index"
            temp_dict["_id"] = row["_id"]
            row.pop("_id")
            # temp_dict["doc"] = row
            temp_dict["_source"] = row

            action_list.append(temp_dict)
        # 塞入資料
        result = helpers.bulk(
            es, action_list, index=index_name, chunk_size=400  # , ignore_status=[404]
        )
        return result

    def es_bulk_update_doc_by_id(self, data_list):
        es = self.es_get_connect()
        index_name = self.index_name
        action_list = []
        for row in data_list:
            temp_dict = {}
            temp_dict["_op_type"] = "update"
            temp_dict["_id"] = row["_id"]
            row.pop("_id")
            temp_dict["doc"] = row

            action_list.append(temp_dict)
        # 塞入資料
        result = helpers.bulk(
            # es, action_list, index=index_name, chunk_size=400, ignore_status=[404]
            es,
            action_list,
            index=index_name,
            chunk_size=400,
            ignore_status=[404],
        )
        return result

    def create_index(self, settings={}, mappings={}, aliases=""):
        es = self.es_get_connect()
        index_name = self.index_name
        body = dict()
        body["settings"] = settings
        body["mappings"] = mappings
        if self.aliases:
            body["aliases"] = aliases
        print(json.dumps(body))  # 可以用json.dumps輸出來看格式有沒又包錯
        es.indices.create(index=index_name, body=body, ignore=[400])

    def create_index_by_template(self, aliases=""):
        es = self.es_get_connect()
        index_name = self.index_name
        body = dict()
        if aliases:
            body["aliases"] = aliases
        print(json.dumps(body))  # 可以用json.dumps輸出來看格式有沒又包錯
        es.indices.create(index=index_name, body=body, ignore=[400])

    def delete_by_query(self, body):
        es = self.es_get_connect()
        index_name = self.index_name
        result = es.delete_by_query(index=index_name, body=body, ignore=[400, 409])
        return result

    def query(self, body):
        es = self.es_get_connect()
        index_name = self.index_name
        result = es.search(index=index_name, body=body, ignore=[400])
        return result

    def get_point_in_time(self):
        es = self.es_get_connect()
        index_name = self.index_name
        pit = es.open_point_in_time(index=index_name, keep_alive="1m")
        return pit["id"]


if __name__ == "__main__":
    print("Call it locally")
