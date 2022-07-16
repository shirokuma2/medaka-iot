import json
import logging
import math
import os
import time
import traceback
# standard
from datetime import datetime, timedelta, timezone
from typing import Callable, List

import firebase_admin
import google.auth
import requests
from fastapi import HTTPException
from firebase_admin import credentials, firestore, initialize_app
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from joblib import Parallel, delayed
from retrying import retry
from tqdm import tqdm

STOP_MAX_ATTEMPT_NUMBER = 1
WAIT_FIXED = 1000


cred = credentials.ApplicationDefault()
initialize_app(cred, options={'projectId': 'exponential-awiiin'})
db = firestore.client()
service_account = os.environ["service_account"]


class Utility:
    def __init__(self):
        # self.discord_api = "https://us-central1-instant-icon-250708.cloudfunctions.net/notify_on_discord"
        self.JST = timezone(timedelta(hours=9), 'JST')
        self.bq = bigquery.Client()
        self.db = db
        self.logging = logging

    def date_obj(self, n: int = 0):
        return datetime.now(self.JST) + timedelta(hours=n)

    @staticmethod
    def from_iso(str_time: str, n: int = 0):
        """
        '2018-12-31T05:00:30.001000' => datetime.datetime(2018, 12, 31, 5, 0, 30, 1000)
        :param str_time:
        :param n:
        :return:
        """
        if n:
            return datetime.fromisoformat(str_time) + timedelta(hours=n)
        return datetime.fromisoformat(str_time)

    def yyyy_mm_dd_hh_mm_ss(self, n: int = 0):
        """
        :return: '2018-12-31 05:00:30'
        """
        if n:
            return self.date_obj(n).strftime("%Y-%m-%d %H:%M:%S")
        return self.date_obj().strftime("%Y-%m-%d %H:%M:%S")

    def year_month_day(self):
        """
        if today 2021-12-01 or 2021-12-10
        :return: [2021, 12, 1] or [2021, 12, 10]
        """
        obj = self.date_obj()
        return obj.year, obj.month, obj.day

    def hour_minute_sec(self):
        """
        if today 2021-12-01 05:30:00 or 2021-12-10 11:00:10
        :return: [5, 30, 0] or [11, 0, 10]
        """
        obj = self.date_obj()
        return obj.hour, obj.minute, obj.second

    # @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    # def discord(self, send: dict):
    #     requests.post(self.discord_api, data=send)

    @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    def read_bq(self, query: str):
        """
        read big query with sql
        :param query: sql code of string
        :return: pandas.dataframe
        """
        return self.bq.query(query).to_dataframe()

    @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    def add_bq(self, df, table: str, schema_dict: dict, dt_col: str = "timestamp"):
        """パーティションテーブルを作成する
        パーティションテーブル: 時間単位でデータ参照が可能
        引数1 df: 格納するデータフレーム
        引数2 table_name: 形式 instant-icon-250708.hoge.foo
        引数3 datetime_col: datetime型で保存する列名 デフォルトはtimestamp row_change_logを基本とする
        必須version: 1.25.0以上
        使用例: table_name, datetime_col = "instant-icon-250708.hoge.foo", "createdAt"
        manager.add_bq(df, table_name, datetime_col)
        https://stackoverflow.com/questions/49542974/bigquery-python-schemafield-with-array-of-structs
        """
        schema = [bigquery.SchemaField(k, v) for k, v in schema_dict.items()]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.HOUR, field=dt_col)
        )
        self.bq.load_table_from_dataframe(df, table, job_config=job_config)
        return "success"

    @staticmethod
    def get_traceback():
        return traceback.format_exc().replace('"', "").replace("'", "").replace("\n", "")

    @staticmethod
    @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    def get_google_auth(to_url: str):
        scopes = ["https://www.googleapis.com/auth/iam", 'https://www.googleapis.com/auth/cloud-platform']
        auth, _ = google.auth.default(scopes=scopes)
        sa = service_account
        server_url = f'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{sa}:generateIdToken'
        token_headers = {'content-type': 'application/json'}
        authed_session = AuthorizedSession(auth)
        body = json.dumps({'audience': to_url.strip()})
        token_response = authed_session.request('POST', server_url, data=body, headers=token_headers)
        jwt = token_response.json()['token']
        return {'Authorization': f'bearer {jwt}'}

    @staticmethod
    def split_data2array(data: list, n: int = 10):
        """
        データをn分割する関数
        :param data:
        :param n:
        :return:
        """
        all_len = len(data)
        div = math.ceil(all_len / n)
        div = 1 if div == 0 else div
        array = [data[i: i + div] for i in range(0, all_len, div)]
        return array

    @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    def save(self, data: dict, doc_ref: google.cloud.firestore_v1.document.DocumentReference):
        doc_ref.set(data, merge=True)
        return "success"

    @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
    def error_handling(self, params: dict, func_name: str, header: str, ch_name: str):
        ch = {
            "outline": "971662335737479228",
            "ex_error": "843505451471863829"
        }

        params = params if params else {}
        error = {
            "traceback": self.get_traceback(),
            "created_at": self.yyyy_mm_dd_hh_mm_ss(),
            "func_name": func_name,
            "header": header,
            **params
        }

        doc_ref = self.db.collection("error_back").document()
        error["doc_id"] = doc_ref.id
        doc_ref.set(error)

        url = self.base_gcf_url + "notify_on_discord"

        send_data = {
            "channel_id": ch.get(ch_name, "ex_error"),
            "content": f"<@everyone> {error['doc_id']}",
            "title": f"{func_name}のエラー通知",
            "description": f"traceback: {error['traceback']}, header: {header}"
        }
        requests.post(url, data=send_data)

#
# @retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
# def parallel_processing(data_list: list, n_jobs: int = -1):
#     result_list = Parallel(n_jobs=n_jobs)(delayed(create_task)(**prm) for prm in tqdm(data_list))
#     if len(result_list) == len([r for r in result_list if r["result"] == "success"]):
#         return result_list
#     raise ValueError("some failures")


@retry(stop_max_attempt_number=STOP_MAX_ATTEMPT_NUMBER, wait_fixed=WAIT_FIXED)
async def verify_tid(tid: str):
    if tid != "tid":
        raise HTTPException(status_code=200, detail="different tid")


def parallel(function: Callable, array: List[dict], n_jobs: int = -1, progress: bool = True):
    if progress:
        return Parallel(n_jobs=n_jobs)(delayed(function)(**prm) for prm in tqdm(array))
    return Parallel(n_jobs=n_jobs)(delayed(function)(**prm) for prm in array)
