#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2019-04-29 11:46:36
# @Author  : Draco (you@example.org)
# @Link    : link
# @Version : 1.0.0

import os
import requests
from bs4 import BeautifulSoup
import redis
import json
import Tools
from Configs import redisConf
import time
from urllib import parse

r = redis.Redis(**redisConf)


class MaoYan:
    # 初始化
    def __init__(self):
        self.session = requests.session()
        self.baseUrl = 'https://maoyan.com/films'

        self.cookieStr = '__mta=154391175.1556502605615.1556503015453.1556503178118.17; _lxsdk_cuid=16a66c8707ac8-0b6232dccb4144-f353163-1fa400-16a66c8707bc8; uuid_n_v=v1; uuid=1BBD0BF06A2111E98B0F2DCDFDB75A6212E6095B655242F1890E79CAA3B08938; _lxsdk=1BBD0BF06A2111E98B0F2DCDFDB75A6212E6095B655242F1890E79CAA3B08938; _csrf=267f1faf04cc6915741e085e4af2e22d1609ac4fc1172d398002380ec492f0f6; lt=qpc-3tCI-Yz9jfUyUbh3zCj6l0oAAAAAUggAAGV4Q4IYFFX0T-A1eeselQc3tE2CjZaa-wVClwbPr-DqgHoF52AX5PIhP7uArgMY1Q; lt.sig=9VMp6Zrz1-OwKTTk_D_2X-WMk3Q; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; __mta=154391175.1556502605615.1556503178118.1557128602109.18; _lxsdk_s=16a8c052e13-9b2-37f-284%7C%7C45'

        cookie_dict = Tools.dictToCookie(self.cookieStr)
        self.session.cookies = requests.utils.cookiejar_from_dict(
            cookie_dict, cookiejar=None, overwrite=True)

    # 获取网页的电影列表
    def getMovieList(self, htmls):
        movieList = htmls.select('.movies-list .movie-list dd')
        for item in movieList:
            # 电影
            title = item.find(
                'div', attrs={"class": "channel-detail movie-item-title"}).get('title')
            # 封面
            cover = item.find_all('img')[1].get('data-src')
            # 评分
            grade = item.find(
                'div', attrs={"class": "channel-detail channel-detail-orange"}).get_text()
            screen = item.find('div', attrs={'class':'movie-ver'}).find('i').get('class')[0] if item.find('div', attrs={'class':'movie-ver'}).find('i') else ''
            # 猫眼电影ID
            movieid = item.find('a', attrs={"data-act":"movie-click"}).get('href').replace('/films/', '')

            result = parse.urlparse(self.url)
            query_dict = parse.parse_qs(result.query)

            catId = int(query_dict['catId'][0]) if 'catId' in query_dict else 0
            sourceId = int(query_dict['sourceId'][0]) if 'sourceId' in query_dict else 0
            yearId = int(query_dict['yearId'][0]) if 'yearId' in query_dict else 0
            if catId > 0:
                ttype = 1
            elif sourceId > 0:
                ttype = 2
            elif yearId > 0:
                ttype = 3
            else:
                ttype = 0

            yield({
                'title': title,
                'cover': cover,
                'grade': grade,
                'screen': screen,
                'catId': catId,
                'sourceId': sourceId,
                'yearId': yearId,
                'type': ttype,
                'movieid': movieid
            })

    # 获取下一页链接
    def getNextPageUrl(self, htmls):
        nextPageUrl = htmls.find('a', text='下一页')
        return nextPageUrl.get('href') if nextPageUrl else ''

    # 爬虫入口
    def run(self, url = ''):
        # url='?showType=3&offset=2010' 经典影片
        # https://maoyan.com/films?showType=3&catId=3&offset=2010 爱情
        self.url = self.baseUrl + url
        print('获取网页：' + self.url)

        content = Tools.getHtmlContent(self.session, self.url)
        if not content:
            print('网页内容获取失败')
            return

        # 主动闭合标签避免结构异常
        content = content.replace('<dd', '</dd><dd')
        soup = BeautifulSoup(content, 'lxml')
        movieList = self.getMovieList(soup)

        for item in movieList:
            print("电影：{}，封面：{} \r\n评分：{}，类型：{}".format(
                item['title'], item['cover'], item['grade'], item['screen']))
            print('----------------------------------------')
            r.lpush('maoyan_movie', json.dumps(item, ensure_ascii=False))
            r.lpush('maoyan_movie_type', json.dumps(item, ensure_ascii=False))
            time.sleep(0.1)

        # 获取下一页链接
        nextPageUrl = self.getNextPageUrl(soup)
        print('下一页链接：' + nextPageUrl)

        if nextPageUrl:
            Tools.dormancy(5, 10)
            self.run(nextPageUrl)

if __name__ == '__main__':
    quelist = [
        '?showType=3&catId=3',
        '?showType=3&catId=2',
        '?showType=3&catId=4',
        '?showType=3&catId=1',
        '?showType=3&catId=6',
        '?showType=3&catId=7',
        '?showType=3&catId=10',
        '?showType=3&catId=5',
        '?showType=3&catId=8',
        '?showType=3&catId=11',
        '?showType=3&catId=9',
        '?showType=3&catId=12',
        '?showType=3&catId=14',
        '?showType=3&catId=15',
        '?showType=3&catId=16',
        '?showType=3&catId=17',
        '?showType=3&catId=18',
        '?showType=3&catId=19',
        '?showType=3&catId=20',
        '?showType=3&catId=21',
        '?showType=3&catId=23',
        '?showType=3&catId=24',
        '?showType=3&catId=25',
        '?showType=3&catId=13',
        '?showType=3&catId=100',


        '?showType=3&sourceId=2',
        '?showType=3&sourceId=3',
        '?showType=3&sourceId=7',
        '?showType=3&sourceId=6',
        '?showType=3&sourceId=10',
        '?showType=3&sourceId=13',
        '?showType=3&sourceId=9',
        '?showType=3&sourceId=8',
        '?showType=3&sourceId=4',
        '?showType=3&sourceId=5',
        '?showType=3&sourceId=14',
        '?showType=3&sourceId=16',
        '?showType=3&sourceId=17',
        '?showType=3&sourceId=11',
        '?showType=3&sourceId=19',
        '?showType=3&sourceId=20',
        '?showType=3&sourceId=21',
        '?showType=3&sourceId=100',


        '?showType=3&yearId=100',
        '?showType=3&yearId=14',
        '?showType=3&yearId=13',
        '?showType=3&yearId=12',
        '?showType=3&yearId=11',
        '?showType=3&yearId=10',
        '?showType=3&yearId=9',
        '?showType=3&yearId=8',
        '?showType=3&yearId=7',
        '?showType=3&yearId=6',
        '?showType=3&yearId=5',
        '?showType=3&yearId=4',
        '?showType=3&yearId=3',
        '?showType=3&yearId=2',
        '?showType=3&yearId=1',
    ]

    MaoYan = MaoYan()
    for i in quelist:
        MaoYan.run(i)
