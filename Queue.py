#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2019-04-30 17:16:32
# @Author  : Draco (you@example.org)
# @Link    : link
# @Version : 1.0.0

import os
import queue
import threading
import time
import redis
from Configs import redisConf,mysqlConf
import Tools
import json
import pymysql


def storage(i, workQueue):
    thredname = 'T' + str(i + 1)
    print('电影入库线程-%s-启动...\n\r' % thredname)

    while True:
        if workQueue.empty():
            print('线程-%s休眠10秒\n\r' % thredname)
            time.sleep(10)
            continue

        try:
            movieData = workQueue.get()
            title = movieData['title']
            cover = movieData['cover']
            grade = movieData['grade']
            screen = movieData['screen']
        except Exception as e:
            print('get workQueue Err:' + str(e))
            continue
        else:
            db = Tools.dbConn(mysqlConf)
            if db == False:
                continue

            thiTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            findSql = "SELECT * FROM `maoyan_movies` WHERE `title`='%s'" % pymysql.escape_string(title)
            dbresult = Tools.dbFind(db, findSql)

            if not dbresult:
                insertSql = "INSERT INTO `maoyan_movies`(`title`, `cover`, `grade`, `screen`, `create_time`, `update_time`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')".format(
                    pymysql.escape_string(title),
                    cover,
                    0 if grade == '暂无评分' else grade,
                    screen,
                    thiTime,
                    thiTime
                )
                result = Tools.dbInsert(db, insertSql)
                if result:
                    print('线程-%s 电影：%s 入库成功\r\n' % (thredname, title))
                else:
                    print('线程-%s 电影：%s 入库失败\r\n' % (thredname, title))
            else:
                updateSql = "UPDATE `maoyan_movies` SET `title`='{}',`cover`='{}',`grade`='{}',`screen`='{}',`update_time`='{}' WHERE `id`={}".format(
                    pymysql.escape_string(title),
                    cover,
                    0 if grade == '暂无评分' else grade,
                    screen,
                    thiTime,
                    dbresult['id']
                )
                result = Tools.dbUpdate(db, updateSql)
                if result:
                    print('线程-%s 电影：%s 修改成功\r\n' % (thredname, title))
                else:
                    print('线程-%s 电影：%s 修改失败\r\n' % (thredname, title))

            time.sleep(0.1)





if __name__ == '__main__':

    r = redis.Redis(**redisConf)
    # 创建队列
    workQueue = queue.Queue()

    # 启动线程
    for i in range(6):
        threads = threading.Thread(target=storage, args=(i, workQueue))
        threads.setDaemon(True)
        threads.start()
        # threads.join()

    print('消息监听线程启动...')
    while True:
        try:
            tmpdata = r.rpop('maoyan_movie')
            if not tmpdata:
                time.sleep(10)
                continue

            print('获取数据为：' + tmpdata)
            tmpdata = json.loads(tmpdata)
            workQueue.put(tmpdata)
            time.sleep(0.1)
        except Exception as e:
            print('获取数据错误：' + str(e))
            continue

