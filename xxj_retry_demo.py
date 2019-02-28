# coding:utf-8
__author__ = 'xxj'

import requests
import time
import os
import re
import Queue
import threading
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from rediscluster import StrictRedisCluster
import lxml.etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
    # 'Host': 'newgame.17173.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
}
PROXY_IP_Q = Queue.Queue()    # 代理ip队列


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    wbsearch_and_bdnews_proxy_length = r.llen('spider:wbsearch_and_bdnews:proxy')  # wbsearch_and_bdnews
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中wbsearch_and_bdnews的代理ip长度：', wbsearch_and_bdnews_proxy_length
    if wbsearch_and_bdnews_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(wbsearch_and_bdnews_proxy_length):
        ip = r.lpop('spider:wbsearch_and_bdnews:proxy')
        proxies = {
            'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
            # 'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


# -------无ip限制------
def get(url, count):
    for i in xrange(count):
        response = r(url, i)
        if response is None:    # 异常
            pass
        elif response.status_code == 200:
            return response
    return None


def r(url, i):
    try:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'url：', url, 'count：', i
        response = requests.get(url=url, headers=headers, timeout=10)
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException', 'url：', url
        response = None
        time.sleep(2)
    return response


# -------ip限制------
def get(url, proxies, count):
    for i in xrange(count):
        response = r(url, proxies, i)
        if response is None:    # 出现非ip被限制异常（实现重试）
            pass
        elif response == 'ip error':    # 出现ip被封现象时（实现切换ip并重试）
            proxies = PROXY_IP_Q.get()
            print '出现ip被限制，切换代理ip', proxies
        elif response.status_code == 200:
            return response
    return None


def r(url, proxies, i):
    try:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'url：', url, 'proxies：',proxies,  'count：', i
        response = requests.get(url=url, headers=headers, proxies=proxies,  timeout=10)
    # 具体的异常根据实际项目来判断是否为ip被限制导致的异常（或根据响应的相关信息（如：状态码，内容信息等）判断为ip被限制）等都可以在这里实现判断并返回相关参数，从而实现ip的切换与重试
    except ConnectTimeout as e:    # 出现ip被限制时
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ConnectTimeout异常', 'url：', url
        return 'ip error'
    except ConnectionError as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ConnectionError异常', 'url：', url
        return 'ip error'
    except BaseException as e:    # 出现非ip被封异常
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException', 'url：', url
        response = None
        time.sleep(2)
    return response