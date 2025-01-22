import os
import jieba
import re
import unicodedata


class jieba_obj:
    def __init__(self, dict_path="", stopword_path=""):
        root_path = os.path.dirname(__file__)
        if not dict_path:
            # self.dict_path = os.path.join(root_path, "static/jieba_dict/", "dict.txt.big.mix.txt")
            self.dict_path = os.path.join(root_path, "static/jieba_dict/", "dict.txt.big.tw_nerd.txt")
        else:
            self.dict_path = dict_path
        if not stopword_path:
            self.stopword_path = os.path.join(root_path, "static/jieba_dict/", "stopwords.txt")
        else:
            self.stopword_path = stopword_path
        self.jieba_obj = self.jieba_init()
        self.stopword_list = self.get_stopword_list()

    def jieba_init(self):
        jieba.set_dictionary(self.dict_path)
        jieba_obj = jieba.initialize()
        return jieba_obj

    def get_stopword_list(self):
        stopword_list = []
        with open(self.stopword_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for tag in lines:
                tag = tag.strip()
                if not tag:
                    continue
                stopword_list.append(tag)
        return stopword_list

    def is_only_punctuation(self, s):
        # 定义一个正则表达式模式，用于匹配非字母和非数字的字符
        pattern = r'^[^\w\s]+$'
        return bool(re.match(pattern, s))

    def full2half(self, c: str) -> str:
        return unicodedata.normalize("NFKC", c)

    def extract_keyword_by_jieba(
        self,
        text,
        cut_all=False,
        min_len=1,
        lowercase=True,
        skip_stopword=False,
        skip_space=True,
        skip_punctuation=True,
        skip_int=True,
        full2half=True,
    ):
        if lowercase:
            text = text.lower()
        result_list = []
        rank_result = jieba.cut(text, cut_all=cut_all, HMM=False)
        for i in rank_result:
            if full2half:
                i = self.full2half(i)
            if skip_space:
                i = i.strip()
            if not i:
                continue
            if len(i) < min_len:
                continue
            if skip_int:
                if i.isdigit():
                    continue
            if skip_punctuation:
                if self.is_only_punctuation(i):
                    continue
            if skip_stopword:
                if i in self.stopword_list:
                    continue
            result_list.append(i)
        return result_list
