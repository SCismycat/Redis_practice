#!/usr/bin/env python
# encoding: utf-8
'''
@author: Leslee
@contact: leelovesc@gmail.com
@time: 2020/3/4 下午7:24
@desc:
'''
import time
import unittest

ONE_WEEK_IN_SECONDS = 7*86400 # 投票文章的过期时间
VOTE_SCORE = 432
# 对文章进行投票
def article_vote(conn,user,article):
    # 计算过期时间
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:',article) < cutoff:
        return
    # 获取文章id
    article_id = article.partition(':')[-1]
    # 如果用户第一次为该文章投票,则增加文章投票数和评分
    if conn.sadd('voted:'+article_id,VOTE_SCORE):
        conn.zincrby('score:',VOTE_SCORE,article)
        conn.hincrby(article,'votes',1)
    if conn.srem('voted:'+article_id,VOTE_SCORE):
        conn.zincrby('score:',-VOTE_SCORE,article)
        conn.hincrby(article,'votes',-1)



def post_article(conn,user,title,link):
    article_id = str(conn.incr('article:'))

    voted = 'voted:'+article_id
    conn.sadd(voted,user)
    conn.expire(voted,ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:'+article_id
    conn.hmset(article,{
        'title':title,
        'link':link,
        'poster':user,
        'time:':now,
        'votes':1,
    })
    conn.zadd('score:',{article:now+VOTE_SCORE})
    conn.zadd('time:',{article: now})
    return article_id

ARTICLES_PER_PAGE = 25
def get_articles(conn,page,order='score:'):
    start = (page-1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1
    ids = conn.zrevrange(order,start,end)
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles

def add_remove_groups(conn,article_id,to_add=[],to_remove=[]):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:'+group, article)
    for group in to_remove:
        conn.srem('group:'+group, article)

def get_group_articles(conn,group,page,order='score:'):
    key = order + group
    if not conn.exists(key):
        conn.zinterstore(key,
                         ['group:'+group,order],
                         aggregate='max',)
        conn.expire(key,60)
    return get_articles(conn,page,key)

class Test(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        del self.conn
        print()

    def test_article_functionality(self):
        conn = self.conn
        import pprint
        article_id = str(post_article(conn,'烽火','雪中悍刀行','www.徐逢年.com'))
        print("insert a article:"+article_id)
        print()
        self.assertTrue(article_id)

        print("")
        r = conn.hgetall('article:'+article_id)
        print(r)
        print()

        article_vote(conn,'我','article:'+article_id)
        print("We voted for the article, it now has votes:", end=' ')
        v = int(conn.hget('article:'+article_id,'votes'))
        print(v)
        print()

        print("最高票数的文章是:")
        articles = get_articles(conn,1)
        print(articles)

        add_remove_groups(conn,article_id,['武侠'])
        print("add article to a new group!")
        articles = get_group_articles(conn,'new-武侠',1)
        print(articles)
        print()

        to_del = (
            conn.keys('time:*') + conn.keys('voted:*') + conn.keys('scores:*') + conn.keys('article:*') + conn.keys('group:*')
        )
        if to_del:
            conn.delete(*to_del)

if __name__ == '__main__':
    unittest.main()