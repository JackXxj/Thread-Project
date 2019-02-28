# coding:utf-8
__author__ = 'xxj'

import requests
import time
import os
import re
import Queue
import threading
from retrying import retry
import lxml.etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
    # 'Host': 'newgame.17173.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
}


def index_page_parse(fileout):
    url = 'http://newgame.17173.com/game-list.html'
    # response = get(url, 10)
    try:
        response = request_retry(url, '获取总页数url：')
    except BaseException as e:
        response = None
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
                try:
                    response = request_retry(url, '索引页url：')
                except BaseException as e:
                    response = None
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
                        detail_page_content = detail_page_parse(game_url, game_time)    # 详情页接口
                        if detail_page_content is not None:
                            # 字段：中文名称、英文名称、游戏类型、开发商、运行平台、测试时间、运营商、游戏简介
                            fileout.write(detail_page_content)
                            fileout.write('\n')
                            fileout.flush()
                else:
                    print '游戏索引页的response is None'
    else:
        print '获取总页数的response is None'


def detail_page_parse(detail_page_url, game_time):
    # response = requests.get(url=detail_page_url, headers=headers, timeout=10)
    # response = get(detail_page_url, 3)    # 所以对请求出现的异常进行重试机制。
    try:
        response = request_retry(detail_page_url, '详情页url：')
    except BaseException as e:
        response = None
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


@retry(stop_max_attempt_number=3, wait_fixed=2000)
def request_retry(url, des):
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), des, url
    response = requests.get(url, headers=headers, timeout=10)
    return response


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

    index_page_parse(fileout)
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
