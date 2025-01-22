import os

# import google.auth
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig
import time
from datetime import date, datetime
import json
import ndjson


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(obj, date):
            return obj.strftime("%Y%m%d")
        elif isinstance(obj, set):
            return list(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class bigquery_database:
    def __init__(
        self,
        secret_file_path,
    ):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = secret_file_path
        self.bq_client = bigquery.Client()

    # 語法查詢函示
    def query_check(self, query):
        bq_client = self.bq_client
        conf = QueryJobConfig()
        if "legacy" in query:
            conf.use_legacy_sql = True
        # conf.dry_run = True #open wile testing
        query_job = bq_client.query(
            query,
            # Location must match that of the dataset(s) referenced in the query.
            location="US",
            job_config=conf,
        )  # API request - starts the query

        while not query_job.done():
            # Wait for the job to complete.
            time.sleep(1)

        # query size
        billed_byte = query_job.total_bytes_processed
        if billed_byte < pow(1024, 3):
            billed_size = str(round(int(billed_byte) / 1024 / 1024, 2)) + " MB"
        else:
            billed_size = str(round(int(billed_byte) / 1024 / 1024 / 1024, 2)) + " GB"
        billed_cost = str(round(int(billed_byte) * 150 / pow(1024, 4), 2)) + " 元台幣"

        # print(query_job.query)
        print(f"state: {query_job.state}")
        print(f"billed: {billed_size}, {billed_cost}")

        return query_job.to_dataframe()
        # return query_job

    def search(self, sql):
        # sql = """
        # #standardsql
        # SELECT * FROM `pixnet-gt.grouptargeting.article_profile_*`
        # WHERE _TABLE_SUFFIX BETWEEN '20230201' AND '20230201'
        # """

        result = self.query_check(sql)

        result_reformat = result.to_json(orient="records")
        result_reformat = json.loads(result_reformat)
        return result_reformat
