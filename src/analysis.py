from __future__ import print_function
import json
import mysql.connector
import nltk
import re
from mysql.connector import errorcode

# After you set up your mySQL database, alter the information in this
# file.
db_config_file = "../config/db_config.json"

neg_words_file = "../config/neg_word_list.txt"


article_data = ("SELECT articleID, published_date, title, text "
                "FROM articles "
                "WHERE ticker_symbol=%(symbol)s")

article_data_by_id = ("SELECT articleID, published_date, title, text, ticker_symbol "
                      "FROM articles "
                      "WHERE articleID=%(id)s;")

comment_data = ("SELECT commentID, content, neg_words "
                "FROM comments;")

write_article_neg = ("UPDATE seeking_alpha.articles "
                     "SET neg_words =%(neg_words)f "
                     "WHERE articleID=%(id)s;")

write_comment_neg = ("UPDATE seeking_alpha.comments "
                     "SET neg_words =%(neg_words)f "
                     "WHERE commentID=%(id)s;")

filter_query = ("SELECT ticker_symbol, COUNT(articleID) as cnt "
                "FROM seeking_alpha.articles "
                "GROUP BY ticker_symbol "
                "HAVING cnt < 50 "
                "ORDER BY cnt DESC;")

filter_query_neg_words = ("SELECT articleID, neg_words "
                          "FROM seeking_alpha.articles "
                          "HAVING neg_words IS NULL;")


class Article:
    def __init__(self, ticker, query_result):
        self.ticker = ticker
        self.id = query_result[0]
        self.date = query_result[1]
        self.title = query_result[2]
        self.text = query_result[3]

    def analyze(self, neg_words):
        return neg_sentiment_percentage(self.text, neg_words)

    def __str__(self):
        return "Article(" + "\n\ttitle: " + self.title + "\n\tpublished: " + str(self.date) + "\n\tticker: " + self.ticker + "\n)"


class Comment:
    def __init__(self, query_result):
        self.id = query_result[0]
        self.text = query_result[1]

    def analyze(self, neg_words):
        return neg_sentiment_percentage(self.text, neg_words)

    def __str__(self):
        return "Comment(" + self.text + ")"


def default_db_config():
    """
    Gets default database configuration.
    """
    return read_json_file(db_config_file)


def read_data_file(filename):
    with open(filename, "r") as f:
        data = f.read()
    return data.split(",")


def write_data_to_file(data, filename):
    with open(filename, "w") as f:
        f.write("".join(map(lambda x: x + ",", data))[:-1])
    return True


def clean_file(fn, output_fn=None):
    output_fn = (fn, output_fn)[output_fn]
    with open(fn, "r") as f:
        contents = set(f.read().split(","))
    with open(output_fn, "w") as f:
        f.write("".join(map(lambda x: x+",", contents)))


def stock_tickers(filename=ticker_file):
    return read_data_file(filename)


def query_database(csr, query, params={}):
    print("Executing query: " + query % params)
    csr.execute(query, params)
    print("\t Fetching...")
    result = csr.fetchall()
    print("\t Complete")
    return result


def get_articles_for_ticker(csr, ticker, query=article_data):
    return list(map(lambda data: Article(ticker, data),
                    query_database(csr, query, {"symbol": ticker})))


def filter_tickers_by_article_num(csr, fn="..\\Seeking Alpha Data\\filtered_tickers.txt"):
    result = query_database(csr, filter_query)
    write_data_to_file(map(lambda r: r[0], result), fn)
    return result


def next_market_day(date):
    return date


def clean_text(text):
    tokens = nltk.word_tokenize(text.replace("-", " "))
    return [word.lower() for word in tokens if word.isalpha()]


def neg_sentiment_percentage(raw_text, neg_words):
    percent = 0.0
    text = clean_text(raw_text)
    n = len(text)
    for word in text:
        if word in neg_words:
            percent += 1.0 / n
    return percent


def analyze_ticker_articles(csr, cnx, ticker, neg_words):
    articles = get_articles_for_ticker(csr, ticker)
    for a in articles:
        percent = a.analyze(neg_words)
        cursor.execute(write_article_neg %
                       {"id": a.id, "neg_words": percent})
    cnx.commit()


def analyze_tickers(csr, cnx, tickers, neg_words):
    for t in tickers:
        analyze_ticker_articles(csr, cnx, t, neg_words)


def get_all_comments(csr):
    comments = query_database(csr, comment_data)
    return list(map(lambda x: Comment(x), comments))


def get_all_articles(csr):
    article_ids = query_database(csr, filter_query_neg_words)
    articles = []
    for aid in map(lambda x: x[0], article_ids):
        for article in query_database(csr, article_data_by_id, {"id": aid}):
            articles.append(Article(article[4], article[:4]))
    return articles


def analyze_comments(csr, cnx, neg_words):
    for c in get_all_comments(csr):
        percent = c.analyze(neg_words)
        cursor.execute(write_comment_neg %
                       {"id": c.id, "neg_words": percent})
    cnx.commit()


def analyze_articles(csr, cnx, neg_words):
    for a in get_all_articles(csr):
        percent = a.analyze(neg_words)
        cursor.execute(write_article_neg %
                       {"id": a.id, "neg_words": percent})
    cnx.commit()

if __name__ == '__main__':
    connection = mysql.connector.connect(**default_db_config())
    cursor = connection.cursor()
    negative_words = set(read_data_file(neg_words_file))

    # # Analyzes all comments in database
    # analyze_comments(cursor, connection, negative_words)
