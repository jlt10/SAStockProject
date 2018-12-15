from __future__ import print_function
import mysql.connector
import requests
import time
import json
from http.cookies import SimpleCookie
from bs4 import BeautifulSoup

##################################
#                                #
#           CONSTANTS            #
#                                #
##################################
# After you set up your mySQL database, alter the information in this
# file.
db_config_file = "../config/db_config.json"

# Log into SA, then copy paste your cookie into this file.
raw_cookie_file = "../config/raw_cookie.txt"

user_agent = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2)' +
                            ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

add_article = ("INSERT INTO articles"
               "(articleID, ticker_symbol, published_date, author_name, title, text, num_likes, includes_symbols)"
               "VALUES (%(articleID)s, %(ticker_symbol)s, %(published_date)s, %(author_name)s, %(title)s, %(text)s,"
               " %(num_likes)s, %(includes_symbols)s)")

add_comment = ("INSERT INTO comments"
               "(articleID, commentID, userID, comment_date, content, parentID, discussionID)"
               "VALUES (%(articleID)s, %(commentID)s, %(userID)s, %(comment_date)s, %(content)s, %(parentID)s,"
               "%(discussionID)s)")

##################################
#                                #
#         DATA CLASSES           #
#                                #
##################################
class Article:
    def __init__(self, _id, a_cookie, a_user_agent):
        """
        Initializes all fields with default values then parses the
        information from the url.
        """
        self._id = _id
        self.ticker = ''
        self.pub_date = '0001-01-01'
        self.author = ''
        self.title = ''
        self.text = ''
        self.includes = ''

        self.comments = []
        self.valid = True
        self._parse_article(a_cookie, a_user_agent)

    def _parse_article(self, a_cookie, a_ua):
        """
        Parses article info from the given url.
        """
        url = "https://seekingalpha.com/article/%s" % self._id
        r = safe_request(url, {})
        r_login = safe_request(url, a_cookie)

        soup_log = BeautifulSoup(r_login.text, 'html.parser')
        # Stops process if article invalid
        primary_about = soup_log.find_all("a", href=True, sasource="article_primary_about")
        if len(primary_about) != 1:
            # Excludes non-single-ticker articles
            print("Invalid Article")
            self.valid = False
            return
        else:
            self.ticker = primary_about[0].text.split()[-1][1:-1]

        # Gets all includes and author
        about = soup_log.find_all("a", href=True)
        for a in about:
            if 'sasource' in a.attrs:
                if a.attrs['sasource'] == "article_about":
                    self.includes += a.text + ","
                elif a.attrs['sasource'] == "auth_header_name":
                    self.author += a.text + ","

        self.includes = self.includes[:-1]
        self.author = self.author[:-1]
        self.title = soup_log.find_all('h1')[0].text
        self.pub_date = soup_log.find_all('time', itemprop="datePublished")[0]['content'][:10]

        # Get Full Article Text
        name_box = BeautifulSoup(r.text, 'html.parser').find_all('p')
        print(name_box)
        try:
            disc_idx = list(filter(lambda i: 'id' in name_box[i].attrs and name_box[i]['id'] == 'a-disclosure',
                                   range(len(name_box))))[0]
        except IndexError:
            disc_idx = len(name_box)
        self.text = ''.join(map(lambda x: x.text + "\n", name_box[:disc_idx]))

    def json(self):
        """
        Returns json representation of an article (for writing
        to the database).
        """
        if self.valid:
            return {
                'articleID': self._id,
                'ticker_symbol': self.ticker,
                'published_date': self.pub_date,
                'author_name': self.author,
                'title': self.title,
                'text': self.text,
                'num_likes': 0,
                'includes_symbols': self.includes
            }

        return {}


class Comment:
    def __init__(self, article_id, comment):
        self.articleID = article_id
        self.commentID = comment['id']
        self.userID = comment['user_id']
        self.date = comment['created_on'][:10]
        self.text = comment['content']
        self.parentID = comment['parent_id']
        self.discussionID = comment['discussion_id']

        self.children_ids = comment['children']

    def get_children(self):
        """
        Recursively returns an array of all the children of the comment.
        """
        children = []
        for i in self.children_ids:
            child = Comment(self.articleID, self.children_ids[i])
            children.append(child)
            children.extend(child.get_children())
        return children

    def json(self):
        return {
            'articleID': self.articleID,
            'commentID': self.commentID,
            'userID': self.userID,
            'comment_date': self.date,
            'content': self.text.encode('ascii', errors='ignore').decode(),
            'parentID': self.parentID,
            'discussionID': self.discussionID
        }


##################################
#                                #
#        FILE FUNCTIONS          #
#                                #
##################################
def read_json_file(filename):
    """
    Reads a json formatted file.
    """
    with open(filename) as f:
        try:
            data = json.loads(f.read())
        except:
            data = {}
    return data


def write_json_file(json_data, filename):
    """
    Writes a json to a file.
    """
    try:
        str_data = json.dumps(json_data)
        with open(filename, "w") as f:
            f.write(str_data)
        return True
    except MemoryError:
        return False


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


def default_db_config():
    """
    Gets default database configuration.
    """
    return read_json_file(db_config_file)


def safe_request(url, cookie):
    """
    Continues trying to make a request until a certain amount of
    tries have failed.
    """
    count = 0
    r = ""
    # Adjust this number if a certain amount of failed attempts
    # is acceptable
    while count < 1:
        try:
            r = requests.get(url, cookies=cookie, headers=user_agent)
            if r.status_code != 200:
                print(r.status_code, "blocked")
                count += 1
            else:
                break
        except requests.exceptions.ConnectionError:
            print("timeout", url)
            time.sleep(1)
    return r


def get_comment_jsons(article_id, cookie):
    """
    Returns all comments for the given article as array of
    jsons.
    """
    url = "https://seekingalpha.com/account/ajax_get_comments?id=%s&type=Article&commentType=" % article_id
    r = safe_request(url, cookie)
    comments = []

    if r.status_code != 404:
        res = json.loads(r.text)
        for comment in res['comments'].values():
            c = Comment(article_id, comment)
            comments.append(c.json())
            comments.extend(map(lambda x: x.json(), c.get_children()))

    return comments


def try_add_comment(com_jsons, cursor, article_id):
    """
    Given array of comment jsons, adds comments to database.
    """
    if not com_jsons:
        print("\t No comments found for " + article_id)

    for c in com_jsons:
        try:
            cursor.execute(add_comment, c)
        except mysql.connector.DatabaseError as err:
            if not err.errno == 1062:
                print("Wrong Comment Format: " + c["id"])


def try_add_article(art_json, cursor):
    """
    Given an article json, tries to write that article to database.
    """
    try:
        cursor.execute(add_article, art_json)
    except mysql.connector.errors.IntegrityError:
        print("Duplicate Article")


def try_add_db(art_json, com_jsons, cursor, article_id):
    try_add_article(art_json, cursor)
    try_add_comment(com_jsons, cursor, article_id)


def gather_mysql_data(article_fn, start=0, stop=None, comments_only=False):
    """
    Given a file with Seeking Alpha article ids separated by commas, iterates
    through the article ids in the article and records the article and comment
    data in the mysql database.
    """
    config = default_db_config()
    cookie = default_cookie()

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    with open(article_fn) as f:
        articles = f.read().split(",")
    i, total = start+1, float(len(articles))

    for a in articles[start: stop]:
        if comments_only:
            com_jsons = get_comment_jsons(a, cookie)
            try_add_comment(com_jsons, cursor, a)
        else:
            art_json = Article(a, cookie, user_agent).json()
            if art_json:
                com_jsons = get_comment_jsons(a, cookie)
                try_add_db(art_json, com_jsons, cursor, a)

        cnx.commit()
        print("%0.4f" % (i/total*100), "%\t Article idx:", i-1)
        i += 1

    cursor.close()
    cnx.close()

if __name__ == '__main__':
    # Collection has not been updated in a long time so there are some
    # aspects of the pipeline that do not seem to work anymore. While
    # writing to the database seems fine, getting the full article text seems
    # to be not working again.
    a = Article("239509", default_cookie(), user_agent)
    print(a.json())

    # Do NOT run collection of articles before that bug has been fixed because
    # you will overwrite your database with the truncated text version of these
    # articles.




