import requests
import time
import re
from http.cookies import SimpleCookie


raw_cookie_file = "../config/raw_cookie.txt"


def browser_cookie(rawcookie):
    cookie = SimpleCookie()
    cookie.load(rawcookie)

    # reference: https://stackoverflow.com/questions/32281041/converting-cookie-string-into-python-dict
    # Even though SimpleCookie is dictionary-like, it internally uses a Morsel object
    # which is incompatible with requests. Manually construct a dictionary instead.
    cookies = {}
    for key, morsel in cookie.items():
        cookies[key] = morsel.value
    return cookies


def default_cookie():
    """
    Gets cookie from the raw cookie file.
    """
    with open(raw_cookie_file) as f:
        rc = "".join(f.readlines())
        return browser_cookie(rc)


def matched_url(input_str):
    pattern = 'article/[0-9]*'
    matchObject = re.findall(pattern, input_str, flags=0)
    return matchObject


def past_urls_per_symbol(symbol, start_page, end_page):
    """
        symbol: Any valid symbol on US Stock Exchange [NASDAQ, NYSE, etc.]
        start_page: if crawling entire history, set to 1
        end_page: if crawling entire history, set to a very large number, 10000
                    the program will break the for loop once there is no matching
                    URL in a page
    """
    past_urls = []
    headers = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

    for i in range(start_page, end_page):
        url = 'https://seekingalpha.com/symbol/' + symbol + '/more_focus?page=' + str(i)
        response = requests.get(url, cookies=default_cookie(), headers=headers)
        if response.status_code == 200:
            matched = matched_url(response.text)
            if not matched:
                break
            past_urls += matched
        else:
            print('status code is %d, something is wrong' % (response.status_code))

        # avoid rate limitation, if status code == 429, increase sleep interval
        time.sleep(0.5)
    return past_urls


def urls_to_ids(urls):
    return list(map(lambda x: x.split("/")[-1], urls))


if __name__ == '__main__':
    # Example: ids of SA articles about Apple from the first three pages of results
    print(urls_to_ids(past_urls_per_symbol("AAPL", 1, 3)))
