import requests
from bs4 import BeautifulSoup


def return_hello_world():
    return "Hello world!"


def return_what_you_say(text: str) -> str:
    temp_text = f"you are saying: {text}"
    return temp_text


def parse_url(url: str) -> str:
    a = requests.get(url)
    a = a.text
    return a


def parse_pixnet_hot_article(url: str):
    a = parse_url(url)
    soup = BeautifulSoup(a, 'html.parser')
    temp_result = soup.find_all('div', {"class": "hot-index-box__link"})
    temp_list = []
    for i in temp_result:
        temp_list.append(i.string)
    return temp_list
