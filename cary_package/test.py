import requests


def return_hello_world():
    return "Hello world!"


def return_what_you_say(text: str) -> str:
    temp_text = f"you are saying: {text}"
    return


def parse_url(url: str) -> str:
    a = requests.get(url)
    a = a.text
    return a
