#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2020/3/9 23:17
# @Author  : Leslee
import threading
import time
import json
import unittest
import urllib.parse
import uuid


def to_bytes(x):
    return x.encode() if isinstance(x,str) else x

def to_str(x):
    return x.decode() if isinstance(x,bytes) else x
# 获取并返回token对应的用户,用户登录表(token-user id,hash)
def check_token(conn,token):
    return conn.hget('login:',token)

def update_token(conn,token,user,item=None):
    timestamp = time.time()
    conn.hset('login:',token,user) # 维护一个token-user的映射
    conn.zadd('recent:',{token:timestamp}) # 用户token和当前时间的ZSET
    if item:
        conn.zadd('viewed:'+token,{item:timestamp}) # 用户浏览过的商品和时间。
        conn.zremrangebyrank('viewed:'+token,0,-26) # 最多保存25个小时。

QUIT = False
LIMIT = 10000000
# 定期清除用户
def clean_session(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size-LIMIT,100) # 看看表里的用户数量是否超过限制
        tokens = conn.zrange('recent:',0,end_index-1)
        session_keys = []
        for token in tokens:
            token = to_str(token)
            session_keys.append('viewed:'+token)
        conn.delete(*session_keys)
        conn.hdel('login:',tokens)
        conn.zrem('recent:',tokens)
# 实现购物车
def add_to_shopping_car(conn,session,item,count):
    if count <= 0:
        conn.hrem('cart:'+session,item)
    else:
        conn.hset('cart:'+session,item,count)
# 上面的定期清理用户，也需要清理对应的购物车。
def clean_full_session(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size-LIMIT,100) # 看看表里的用户数量是否超过限制
        sessions = conn.zrange('recent:',0,end_index-1)
        session_keys = []
        for sess in sessions:
            sess = to_str(sess)
            session_keys.append('viewed:'+sess)
            session_keys.append('cart:'+sess)
        conn.delete(*session_keys)
        conn.hdel('login:',sessions)
        conn.zrem('recent:',sessions)
# 缓存页面
def cache_request(conn,request,callback):
    if not can_cache(conn,request):
        return callback(request)

    page_key = 'cache:'+hash_request(request)
    content = conn.get(page_key)

    if not content:
        content = callback(request)
        conn.setex(page_key,300,content)
    return content

# 缓存数据行
## 要先有调度函数和延时函数
def schedule_row_cache(conn,row_id,delay):
    conn.zadd('delay:',{row_id:delay})
    conn.zadd('schedule:',{row_id,time.time()})

def cache_row(conn):
    while not QUIT:
        next = conn.zrange('schedule:',0,0,withscores=True) # 尝试获取下一个需要被缓存的数据行，以及该行调度时间戳，命令返回一个包含tuple的列表。
        now = time.time()
        if not next or next[0][1] > now:# 没有需要缓存的就休眠50ms
            time.sleep(.05)
            continue
        row_id = next[0][0]
        row_id = to_str(row_id)
        delay = conn.zscore('delay:',row_id) #获取下次调度的延迟时间
        if delay <= 0:
            conn.zrem('delay:',row_id)
            conn.zrem('schedule:',row_id)
            conn.delete('inv:'+row_id)
            continue

        row = Inventory.get(row_id)# 读取数据行
        conn.zadd('schedule:',{row_id:now+delay}) # 增加调度时间
        row = {to_str(k):to_str(v) for k,v in row.to_dict().items()}
        conn.set('inv:'+row_id,json.dumps(row)) #设置缓存值

def update_token(conn,token,user,item=None):
    timestamp = time.time()
    conn.hset('login:',token,user) # 维护一个token-user的映射
    conn.zadd('recent:',{token:timestamp}) # 用户token和当前时间的ZSET
    if item:
        conn.zadd('viewed:'+token,{item:timestamp}) # 用户浏览过的商品和时间。
        conn.zremrangebyrank('viewed:'+token,0,-26) # 最多保存25个小时。
        conn.zincrby('viewed:',-1,item)

def rescale_viewed(conn):
    while not QUIT:
        conn.zremrangebyrank('viewed:',2000,-1)
        conn.zinterstore('viewed:',{'viewed:':.5})
        time.sleep(300)

def can_cache(conn,request):
    item_id = extract_item_id(request)
    if not item_id or is_dynamic(request):
        return False
    rank = conn.zrank('viewed:',item_id)
    return rank is not None and rank<10000

def extract_item_id(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]

def is_dynamic(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]

def hash_request(request):
    return str(hash(request))

class Inventory(object):
    def __init__(self,id):
        self.id = id

    @classmethod
    def get(cls,id):
        return Inventory(id)

    def to_dict(self):
        return {'id':self.id,'data':'data to cached..','cached':time.time()}

class Test(unittest.TestCase):

    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        conn= self.conn
        to_del = (
            conn.keys('login:*')+conn.keys('recent:*')+conn.keys('viewed:*')+
            conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*')+
            conn.keys('schedule:*')+conn.keys('inv:*')
        )
        if to_del:
            self.conn.delete(*to_del)

        del self.conn
        global QUIT,LIMIT
        QUIT = False
        LIMIT = 10000000
        print("")

    def test_login_cookies(self):
        conn = self.conn
        global LIMIT,QUIT
        token = str(uuid.uuid4())

        update_token(conn,token,'username','itemX')
        print("我们刚才登录并更新了token",token)
        print("用户是：",'username')
        print(" ")

        print("当我们遇到这个token，我们怎么确定是谁？")
        r = check_token(conn,token)
        print(r)
        print()

        print("删除最大数量的cookie")
        print("开启一个线程来执行清理操作，直到关闭该线程")
        LIMIT = 0
        t = threading.Thread(target=clean_session,args=(conn,))
        t.setDaemon(1)
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("清理会话的线程还活着呐？！")

        s = conn.hlen('login:')
        print("当前会话数数量为：",s)

    def test_shopping_cart_cookie(self):
        conn = self.conn
        global LIMIT,QUIT

        token = str(uuid.uuid4())

        print("刷新会话。。")
        print()
        update_token(conn,token,'人名',"w威士忌")
        print("增加一件商品到购物车")
        add_to_shopping_car(conn,token,"itemY",3)
        r = conn.hgetall('cart:'+token)
        print("购物车现在还有：",r)
        print()

        print("开始清理会话和清理购物车")
        LIMIT = 0
        t = threading.Thread(target=clean_full_session, args=(conn,))
        t.setDaemon(1)
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("清理会话的线程还活着呐？！")

        s = conn.hgetall('cart:'+token)
        print("当前会话数数量为：", s)


    def test_cache_request(self):
        conn = self.conn
        token =str(uuid.uuid4())

        def callback(request):
            return "content for" + request

        update_token(conn,token,"usename","itemX")
        url = "http://test.com/?item=itemX"
        print("测试一个简单的请求",url)
        result = cache_request(conn,url,callback)
        print()

        print("为了证明我们已经缓存了这个请求，现在不传callback")
        result2 = cache_request(conn,url,None)
        print("我们这里得到了相同的请求，无法缓存")

        self.assertFalse(can_cache(conn,'http://test.com/'))
        self.assertFalse(can_cache(conn,'http://test.com/?item=itemX&_=123'))
    def test_cache_row(self):
        conn = self.conn
        global QUIT
        print("首先，每隔5s开始为itemX调度缓存")
        schedule_row_cache(conn,'itemX',5)
        print("调度情况如下：")
        s = conn.zrange('schedule:',0,-1,withscores=True)
        print(s)
        self.assertTrue(s)

        print("开启缓存线程，开始缓存数据")
        t = threading.Thread(target=cache_row,args=(conn,))
        t.setDaemon(1)
        t.start()

        time.sleep(1)
        print("缓存数据如下：")
        r = conn.get('inv:itemX')
        self.assertTrue(r)

        print("5s后确认")
        time.sleep(5)
        print("注意，现在数据变化了")
        r2 = conn.get('inv:itemX')
        print(r2)

        print("让我们表演强行不缓存")
        schedule_row_cache(conn,'itemX',-1)
        time.sleep(1)
        r = conn.get('inv:itemX')
        print("缓存被清理了吗？", not r)
        self.assertFalse(r)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("线程还活着吗？")

if __name__ == '__main__':
    unittest.main()