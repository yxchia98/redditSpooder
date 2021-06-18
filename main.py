import urllib.parse

import praw
import datetime as dt
import re
import pymongo as pymongo


# personal use script: ZKIZ1S_3H_qqQQ
# secret: J55skYeS0sCjTkrBvu3Iuaf_i8GSDQ


def crawl(subreddit, amount, type, db, duration: str = 'all'):
    reddit = praw.Reddit(client_id='ZKIZ1S_3H_qqQQ', client_secret='J55skYeS0sCjTkrBvu3Iuaf_i8GSDQ',
                         user_agent='spooder', username='lulmaomao', password='P@ssw0rd123')
    subreddit = reddit.subreddit(subreddit)
    db.wallstbets.drop()
    postlist = []
    count = 0
    mentionedStock = ''
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
            print('posts crawled:', count, 'postid:', post.id)
            postlist.append({'_id': post.id, 'user': '[deleted]' if not post.author else post.author.name, 'title': post.title, 'upvotes': post.score,
                             'body': post.selftext, 'url': post.url,
                             'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)

    if type == 'new':
        posts = subreddit.new(limit=amount)
        for post in posts:
            count += 1
            print('posts crawled:', count)
            postlist.append({'_id': post.id, 'user': post.author.name, 'title': post.title, 'upvotes': post.score,
                             'body': post.selftext, 'url': post.url,
                             'time': dt.datetime.utcfromtimestamp(post.created)})
        db.wallstbets.insert_many(postlist)

def getstocklist(db):
    allstocks = []
    stockdict = {}
    count = 0
    for x in db.wallstbets.find():
        stocks = re.findall('\$([A-Z]+)', x['title'])
        if len(stocks):
            allstocks.extend(i for i in stocks if i not in allstocks and len(i) > 1)
    print(allstocks)
    for x in db.wallstbets.find():
        if any(stock in x['title'] for stock in allstocks):
            count += 1
            for stock in allstocks:
                if stock in x['title']:
                    if stock in stockdict.keys():
                        stockdict[stock] += 1
                    else:
                        stockdict[stock] = 1
    sortedstockdict = sorted(stockdict.items(), key=lambda x: x[1], reverse=True)
    print('Posts that contains stocks:', count)
    return sortedstockdict




if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb+srv://adm:" + urllib.parse.quote_plus(
            "P@ssw0rd123") + "@cluster0.qyord.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.reddit
    # crawl('wallstreetbets', 1000, 'top', db, 'week')
    stocklist = getstocklist(db)
    print(stocklist)
