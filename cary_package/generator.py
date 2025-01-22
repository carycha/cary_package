import pandas as pd
import hashlib
import unicodedata
import os
import json
import requests

# 轉product feed html code用
import html
import datetime
import time
import pytz
from os.path import getsize


# 計算jaccord距離使用
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np

import scipy.stats


def get_secret_dict(secret_file_path):
    with open(secret_file_path) as json_file:
        secret_dict = json.load(json_file)
    return secret_dict


def get_date_range_list(start_date, end_date):
    date_list = pd.date_range(start=start_date, end=end_date, freq="D").date.astype(str)
    return date_list


def hash_personal_info(text_input, SALT):
    text_hashed = hashlib.sha512(text_input + SALT).hexdigest()
    return text_hashed


def get_timestamp_now():
    now_timestamp = int(time.time())
    return now_timestamp


def epoch_to_UTC8(epoc_time):
    utc_time = None
    try:
        epoc_time = int(epoc_time)
        utc_time = datetime.datetime.fromtimestamp(
            epoc_time, pytz.timezone("asia/taipei")
        ).strftime("%Y-%m-%dT%H:%M:%S")
        if utc_time:
            utc_time = "{}+08:00".format(utc_time)
        return utc_time
    except:
        return utc_time


def str_to_UTC8(date_str, date_format="%Y%m%d"):
    utc_time = None
    try:
        utc_time = (
            datetime.datetime.strptime(date_str, date_format)
            .replace(tzinfo=pytz.timezone("asia/taipei"))
            .strftime("%Y-%m-%dT%H:%M:%S")
        )

        if utc_time:
            utc_time = "{}+08:00".format(utc_time)
        return utc_time
    except:
        return utc_time


def date_str_to_epoch(date_str, date_format="%Y%m%d"):
    epoch_time = None
    try:
        formated_date = datetime.datetime.strptime(date_str, date_format)
        epoch_time = datetime.datetime.timestamp(formated_date)

        return epoch_time
    except Exception as e:
        return epoch_time


def date_str_to_epoch_tz(date_str, date_format="%Y%m%d"):
    epoch_time = None
    try:
        formated_date = datetime.strptime(date_str, date_format)
        # 定义 UTC+8 时区
        utc_plus_8 = pytz.timezone('Asia/Taipei')  # 使用台北时区，属于 UTC+8
        formated_date = utc_plus_8.localize(formated_date)

        epoch_time = datetime.timestamp(formated_date)

        return epoch_time
    except Exception as e:
        return epoch_time


def get_hour_point_timestamp(timestamp_input):
    timestamp_input = int(timestamp_input)
    unit = 3600
    hour_stamp = timestamp_input - (timestamp_input % unit)
    return hour_stamp


def list_dict_sort(input_list, key, reverse_order=False):
    # return key
    output_list = sorted(input_list, key=lambda i: i[key], reverse=reverse_order)
    return output_list


def decode_text(text):
    text = html.unescape(text)
    # print(text)
    text = unicodedata.normalize("NFKD", text)
    return text


def full2half(c: str) -> str:
    return unicodedata.normalize("NFKC", c)


def check_file_exist(filepath):
    if os.path.isfile(filepath):
        return True
    else:
        return False


def get_folder_file_path_list(folder_path):
    result_list = []
    for dirpath, _, filenames in os.walk(folder_path):
        for f in filenames:
            result_list.append(os.path.abspath(os.path.join(dirpath, f)))
        result_list = sorted(result_list, key=getsize, reverse=True)
    return result_list


def get_file_size(filepath):
    return int(os.path.getsize(filepath))


def weighted_jaccard_similarity(y_pre, s_set):
    def add_space(s):
        return " ".join(list(s)).lower()

    # 将字中间加入空格
    corpus = []
    y_pre = add_space(set(y_pre))
    corpus.append(y_pre)
    # 若s_set輸入的是list，就迴圈處理，若是單列，就直接塞入
    if list == type(s_set[0]):
        for s_row in s_set:
            corpus.append(add_space(set(s_row)))
    else:
        corpus.append(add_space(set(s_set)))

    # 转化为TF矩阵
    cv = CountVectorizer(tokenizer=lambda s: s.split())
    vectors = cv.fit_transform(corpus).toarray()

    # 將y_pre抽出來，將s_set中的值加總後平均(同一個字出現在幾個文本中/總文本數，避免加總後分母暴增)
    y_pre_vector = vectors[0]
    vectors = vectors[1:]
    s_set_count = len(vectors)
    vectors = (vectors.sum(axis=0)) / s_set_count

    merge_vec = np.concatenate([[y_pre_vector], [vectors]])
    # 求交集
    numerator = np.sum(np.min(merge_vec, axis=0))
    # 求并集
    denominator = np.sum(np.max(merge_vec, axis=0))
    # 计算杰卡德系数
    return 1.0 * numerator / denominator


def conver_to_json(text):
    try:
        text = json.loads(text)
    except:
        pass
    return text


def sort_list_dict_by_key_ASC(sessionSet, sort_key="activityTime"):
    # 目前僅適用於ga的資料
    # 輸入 list dict，會依據dict中的activityTime進行升冪排序
    if str == type(sessionSet):
        sessionSet = eval(sessionSet)
    sorted_sessionSet = sorted(sessionSet, key=lambda x: (x[sort_key]))
    return sorted_sessionSet


def get_days_ago_date(input_date, days_ago):
    dt = datetime.datetime.strptime(input_date, "%Y-%m-%d")
    days_ago_date = (dt - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return days_ago_date


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i : i + n]


if __name__ == "__main__":
    print("Call it locally")
