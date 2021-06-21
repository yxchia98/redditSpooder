import urllib.parse

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import praw
import datetime as dt
import re
import pymongo as pymongo


# personal use script: ZKIZ1S_3H_qqQQ
# secret: J55skYeS0sCjTkrBvu3Iuaf_i8GSDQ


def crawl(subreddit, amount, type, db, duration: str = 'all'):
    if not duration:
        duration = 'all'
    reddit = praw.Reddit(client_id='ZKIZ1S_3H_qqQQ', client_secret='J55skYeS0sCjTkrBvu3Iuaf_i8GSDQ',
                         user_agent='spooder', username='lulmaomao', password='P@ssw0rd123')
    subreddit = reddit.subreddit(subreddit)
    db.wallstbets.drop()
    postlist = []
    count = 0
    if type == 'hot':
        posts = subreddit.hot(limit=amount)
        for post in posts:
            count += 1
            print('posts crawled:', count)
            postlist.append({'_id': post.id, 'user': post.author.name, 'title': post.title, 'upvotes': post.score,
                             'body': post.selftext, 'url': post.url,
                             'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)
        # for post in posts:
        #     stocks = re.findall('\$([A-Z]+)', post.title)
        #     if len(stocks):
        #         for mentioned in stocks:
        #             if mentionedStock == '':
        #                 mentionedStock += mentioned
        #             mentionedStock += ',' + mentioned
        #         print(post.title, 'mentioned stocks:', mentionedStock)
        #         mentionedStock = ''

    if type == 'top':
        posts = subreddit.top(duration, limit=amount)
        for post in posts:
            count += 1
            print('posts crawled:', count)
            postlist.append(
                {'_id': post.id, 'user': '[deleted]' if not post.author else post.author.name, 'title': post.title,
                 'upvotes': post.score,
                 'body': post.selftext, 'url': post.url,
                 'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)

    if type == 'new':
        posts = subreddit.new(limit=amount)
        for post in posts:
            count += 1
            print('posts crawled:', count)
            postlist.append({'_id': post.id, 'user': '[deleted]' if not post.author else post.author.name, 'title': post.title, 'upvotes': post.score,
                             'body': post.selftext, 'url': post.url,
                             'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)

    if type == 'rising':
        posts = subreddit.rising(limit=amount)
        for post in posts:
            count += 1
            print('posts crawled:', count)
            postlist.append({'_id': post.id, 'user': '[deleted]' if not post.author else post.author.name, 'title': post.title, 'upvotes': post.score,
                             'body': post.selftext, 'url': post.url,
                             'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)


def filterposts(db):
    allstocks = []
    stockdict = {}
    count = 0
    filteredlist = []
    currentstocks = []
    db.wallstbets_filtered.drop()
    for x in db.wallstbets.find():
        stocks = re.findall('\$([A-Z]+)', x['title'])
        if len(stocks):
            allstocks.extend(i for i in stocks if i not in allstocks and len(i) > 1)
    for x in db.wallstbets.find():
        titlelist = re.split('; |;|, |,|\$| ', x['title'])
        if any(stock in titlelist for stock in allstocks):
            count += 1
            for stock in allstocks:
                if stock in titlelist:
                    if stock in stockdict.keys():
                        stockdict[stock] += 1
                    else:
                        stockdict[stock] = 1
                    currentstocks.append(stock)
            x['stocks'] = ','.join(currentstocks)
            currentstocks.clear()
            filteredlist.append(x)

    sortedstockdict = sorted(stockdict.items(), key=lambda x: x[1], reverse=True)
    db.wallstbets_filtered.insert_many(filteredlist)
    sentimentAnalysis(filteredlist)
    return sortedstockdict


def sentimentAnalysis(stocklist):
    filtered_sentence = []
    filtered_list = []
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')
    stop_words = stopwords.words('english')
    stop_words.append('$')
    stop_words = set(stop_words)
    lemmatizer = WordNetLemmatizer()
    for x in stocklist:
        tokenized_words = word_tokenize(x['title'])
        # for token in tokenized_words:
        #     if token.casefold() not in stop_words:
        #         filtered_sentence.append(token)
        filtered_sentence = [word for word in tokenized_words if word.casefold() not in stop_words]
        lemmatized_sentence = [lemmatizer.lemmatize(word) for word in filtered_sentence]
        print("F:", filtered_sentence, "\nL:", lemmatized_sentence)
        # filtered_list.append(list(filtered_sentence))
        lemmatized_sentence.clear()
        filtered_sentence.clear()
    print(filtered_list)
    return 1


if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb+srv://adm:" + urllib.parse.quote_plus(
            "P@ssw0rd123") + "@cluster0.qyord.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.reddit
    menu = False
    while not menu:
        choice = int(input("----MENU----\n1.Crawl data\n2.Analysis\n3.Exit\nEnter Choice:"))
        if choice == 1:
            crawl_cateory_duration = input('Enter reddit category(new/top/hot/rising), followed by duration(all/month/week/day):')
            crawl_cateory_duration = crawl_cateory_duration.split(',')
            crawlcategory = crawl_cateory_duration[0]
            crawlduration = crawl_cateory_duration[1] if len(crawl_cateory_duration) > 1 else None
            crawlamount = input('Enter amount of posts to crawl:')
            crawl('wallstreetbets', int(crawlamount), crawlcategory, db, crawlduration if crawlduration else None)
        elif choice == 2:
            stocklist = filterposts(db)
            print(stocklist)
        else:
            menu = True
