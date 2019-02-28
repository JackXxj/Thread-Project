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


def index_page_parse(fileout, proxies):
    url = 'http://newgame.17173.com/game-list.html'
    response = get(url, proxies, 4)
    if response is not None:
        response_obj = lxml.etree.HTML(response.text)
        last_url = response_obj.xpath('//ul[@id="yw0"]/li[@class="last"]/a/@href')[0]
        search_obj = re.search(r'\?page=(\d+)', last_url, re.S)
        if search_obj:
            all_page = int(search_obj.group(1))
            print '总页数：', all_page
            # for page in xrange(1, 2):
            for page in xrange(1, all_page+1):
                url = 'http://newgame.17173.com/game-list.html?page={page}'.format(page=page)
                # print time.strftime('[%Y-%m-%d %H:%M:%S]'), '索引页url：', url
                # response = requests.get(url=url, headers=headers, timeout=10)
                response = get(url, proxies, 4)
                if response is not None:
                    response.encoding = 'utf-8'
                    response_obj = lxml.etree.HTML(response.text)
                    game_li = response_obj.xpath('//ul[@class="ptlist ptlist-pc"]/li[@class="item"]')
                    for game in game_li:
                        game_time = game.xpath('.//span[@class="ex"]/text()')[0].strip()    # 测试时间
                        search_obj = re.search(r'\d.*\d', game_time, re.S)
                        if search_obj:
                            game_time = search_obj.group()
                        else:
                            print '正则解析失败'
                            game_time = ''
                        # print '测试时间：', game_time
                        game_href = game.xpath('.//div[@class="c1"]/a/@href')[0]    # 游戏详情页url
                        game_url = 'http://newgame.17173.com{game_href}'.format(game_href=game_href, )
                        detail_page_content = detail_page_parse(game_url, game_time, proxies)    # 详情页接口
                        if detail_page_content is not None:
                            # 字段：中文名称、英文名称、游戏类型、开发商、运行平台、测试时间、运营商、游戏简介
                            fileout.write(detail_page_content)
                            fileout.write('\n')
                            fileout.flush()
                else:
                    print '游戏索引页的response is None'
    else:
        print '获取总页数的response is None'


def detail_page_parse(detail_page_url, game_time, proxies):
    # response = requests.get(url=detail_page_url, headers=headers, timeout=10)
    response = get(detail_page_url, proxies, 3)    # 所以对请求出现的异常进行重试机制。
    if response is not None:
        response.encoding = 'utf-8'
        response_obj = lxml.etree.HTML(response.text)

        chinese_name = response_obj.xpath('//div[@class="con"]/h1/text()')    # 中文名
        if chinese_name:
            chinese_name = chinese_name[0].strip()
        else:
            print '不存在中文名，无效页面'
            return None
        # print '中文名：', chinese_name
        english_name = response_obj.xpath('//div[@class="con"]/h2/text()')    # 英文名
        if english_name:
            english_name = english_name[0].strip()
        else:
            english_name = ''
        # print '英文名：', english_name

        game_type = response_obj.xpath('//ul[@class="list-mater-info"]/li[1]/a/text()')   # 游戏类型
        if game_type:
            game_type = game_type[0].strip()
        else:
            game_type = ''
        # print '游戏类型：', game_type

        developer = response_obj.xpath('//ul[@class="list-mater-info"]/li[3]')[0].xpath('string(.)').replace('\r', '').replace('\n', '').replace('\t', '')  # 开发商
        developer = developer.replace('开发商：', '').strip()
        # print '开发商：', developer

        platforms = response_obj.xpath('//li[@class="item-plat"]/a/@title')    # 游戏平台
        platform = '|er|'.join(platforms)
        # print '游戏平台：', platform

        operator_ls = []  # 运营商列表
        operators = response_obj.xpath('//li[@class="item-operator"]//span')  # 一款游戏下有多个运营商
        for operator in operators[1:]:
            operator_name = operator.xpath('string(.)')  # 每个运营商名称
            operator_ls.append(operator_name)
        operator = '|er|'.join(operator_ls)
        # print '运营商：', operator

        # language = response_obj.xpath('//div[@class="con-language"]/span/text()')  # 游戏语言
        # if language:
        #     language = ''.join(language)
        # else:
        #     language = ''
        #     print '无语言游戏：', detail_page_url  # 无第二个游戏标签
        # print '游戏语言：', language

        # sell_date = response_obj.xpath('//ul[@class="list-mater-info"]/li')    # 发售日期
        # if sell_date:
        #     sell_date = sell_date[-2].xpath('string(.)').replace('\t', '').replace('\r', '').replace('\n', '')
        #     if '发售日期' in sell_date:
        #         sell_date = sell_date.replace('发售日期：', '').strip()
        #     else:
        #         sell_date = ''
        #         print '该标签不是发售日期标签：', detail_page_url
        # else:
        #     sell_date = ''
        #     print '无发售日期：', detail_page_url  # 无倒数第二标签
        # print '发售日期：', sell_date

        # activation_code_ls = []
        # activation_codes = response_obj.xpath('//li[@class="item-testinfo"]//span')  # 激活码
        # for activation_code in activation_codes:
        #     activation_code = activation_code.xpath('string(.)').strip()  # 单独激活码
        #     # print '激活码：', activation_code.strip()
        #     activation_code_ls.append(activation_code)
        # activation_code = '|er|'.join(activation_code_ls)
        # print '激活码：', activation_code

        description = response_obj.xpath('//script[@id="tmpl-game-intro-detail"]/text()')    # 游戏介绍
        if description:
            description = description[0].replace('<div>', '').replace('</div>', '').replace('\t', '').replace('\r', '').replace('\n', '').strip()
        else:
            description = response_obj.xpath('//div[@class="mod-mater-intro"]/p/text()')
            if description:
                description = description[0].replace('\t', '').replace('\r', '').replace('\n', '').strip()
            else:
                description = ''
                print '无游戏介绍：', description
        # print '游戏介绍：', description
        # 字段：运营商、发售日期、游戏语言、激活信息、游戏介绍
        # detail_page_content = '\t'.join([operator, sell_date, language, activation_code, description])
        # 字段：中文名、英文名、游戏类型、开发商、运行平台、测试时间、运营商、游戏介绍
        detail_page_content = '\t'.join([chinese_name, english_name, game_type, developer, platform, game_time, operator, description])
    else:
        print 'response is None'
        detail_page_content = ''
    return detail_page_content


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    date = time.strftime('%Y%m%d')
    dest_path = '/ftp_samba/112/spider/potential_newgame/17173_game/'  # linux上的文件目录
    # dest_path = os.getcwd()    # windows上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    dest_file_name = os.path.join(dest_path, '17173_game_' + date)
    tmp_file_name = os.path.join(dest_path, '17173_game_' + date + '.tmp')
    fileout = open(tmp_file_name, 'w')

    # get_redis_proxy()    # 获取代理ip
    # proxy_len = PROXY_IP_Q.qsize()
    # print '代理ip队列中的数量为：', proxy_len

    # 自定义部分
    for ip in ['122.32.123.24:5557', '122.32.123.24:5558', '122.32.123.24:5559', '122.32.123.24:5556', '122.32.123.24:5555']:
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            # 'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)

    proxies = PROXY_IP_Q.get()
    print '当前代理ip：', proxies

    index_page_parse(fileout, proxies)
    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    main()




# 情景一：针对单线程爬虫的异常重试机制（无ip被封现象）
    # 爬虫过程中所出现的异常分为两种类型（1、请求相关异常：如超时异常等；2、网站页面变化后解析失败的异常）[对于异常1可以采取重试机制；对于异常2而言就应该报错体现出来并调整代码]
    # 重试方案：因为仅仅主要对请求异常进行重试。所以只要对请求进行重试封装即可。（1、可以通过xxj自定义的重试模块实现）


# 情景二：针对单线程爬虫的异常重试机制（有ip被封现象）
    # 思想依旧是针对请求进行重试，在请求的过程中（1、非ip被封的异常：如超时；2、ip被封的异常：如ConnectionError；3、IP被封时的响应状态码；4、返回内容体现了ip被封。）【简而言之：就是一种是非ip被封时的异常现象；另一种就是ip被封的异常现象】
    # 所以对于非ip被封的异常现象进行重试操作；对于ip被封现象时应该实现切换代理ip然后再重试