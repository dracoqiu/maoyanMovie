#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2019-04-29 13:53:54
# @Author  : Your Name (you@example.org)
# @Link    : link
# @Version : 1.0.0

import os
import random
import time
import pymysql

# 获取cookie


def dictToCookie(cookie_str=''):
    cookie_dict = dict()
    if cookie_str:
        for item in cookie_str.split(';'):
            key, value = item.split('=', 1)  # 1代表只分一次，得到两个数据
            cookie_dict[key] = value
    return cookie_dict

# 获取网页内容


def getHtmlContent(session, url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        # 'Referer': None  # 注意如果依然不能抓取的话，这里可以设置抓取网站的host
        'Referer': 'maoyan.com'  # 注意如果依然不能抓取的话，这里可以设置抓取网站的host
    }
    try:
        respone = session.get(url, headers=headers)
        if respone.status_code == 200:
            return respone.content.decode('utf-8')
        return False
    except Exception as e:
        print('getHtmlContent err:' + str(e))
        return False

# 休眠器


def dormancy(start=5, end=10):
    sleepTime = random.randint(start, end)
    print('休眠时间：%s' % (sleepTime))
    arrs = list(range(sleepTime))
    arrs.sort(reverse=True)
    for i in arrs:
        time.sleep(1)
        print('倒计时：' + str(i))

# mysql数据库连接


def dbConn(dbconf):
    try:
        db = pymysql.connect(**dbconf)
        return db
    except Exception as e:
        print('db connect Err:' + str(e))
        return False

# 插入数据库


def dbInsert(db, sql=''):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
        return True
    except Exception as e:
        # 如果发生错误则回滚
        print(sql)
        print(e)
        db.rollback()
        return False

    # 关闭数据库连接
    db.close()

# 数据库查询
def dbFind(db, sql=''):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        result = cursor.fetchone()
        return result
    except Exception as e:
        print(e)
        return False

    # 关闭数据库连接
    db.close()

# 修改数据
def dbUpdate(db,sql=''):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
        return True
    except Exception as e:
        print(e)
        # 发生错误时回滚
        db.rollback()
        return False

    # 关闭数据库连接
    db.close()