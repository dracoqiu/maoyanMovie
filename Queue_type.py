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
    print('电影关系入库线程-%s-启动...\n\r' % thredname)

    while True:
        if workQueue.empty():
            print('线程-%s休眠15秒\n\r' % thredname)
            time.sleep(15)
            continue

        try:
            movieData = workQueue.get()
            title = movieData['title']
            cover = movieData['cover']
            grade = movieData['grade']
            screen = movieData['screen']
            catId = movieData['catId']
            sourceId = movieData['sourceId']
            yearId = movieData['yearId']
            ttype = movieData['type']
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
            rviewNum = 0

            while not dbresult:
                rviewNum = rviewNum + 1
                dbresult = Tools.dbFind(db, findSql)
                print(title + ' - 查询不到电影数据休眠5秒等待入库\n\r')
                print(title + ' - 重试次数%s\n\r' % rviewNum)

                if rviewNum == 30:
                    print(title + ' - 取消入库\n\r')
                    break

                time.sleep(5)

            if dbresult:
                findSql2 = "SELECT * FROM `maoyan_movie_related` WHERE `movie_id`='{}' AND `cat_id`='{}'".format(dbresult['id'], catId)
                dbresult2 = Tools.dbFind(db, findSql2)

                if not dbresult2:
                    insertSql = "INSERT INTO `maoyan_movie_related`(`movie_id`, `cat_id`, `source_id`, `year_id`, `create_time`, `update_time`) VALUES ('{}', '{}', '{}', '{}', '{}', '{}')".format(
                        dbresult['id'],
                        catId,
                        sourceId,
                        yearId,
                        thiTime,
                        thiTime
                    )
                    result = Tools.dbInsert(db, insertSql)
                    if result:
                        print('线程-%s 电影关系：%s 入库成功\r\n' % (thredname, title))
                    else:
                        print('线程-%s 电影关系：%s 入库失败\r\n' % (thredname, title))
                else:
                    if ttype == 1:
                        updateSql = "UPDATE `maoyan_movie_related` SET `cat_id`='{}' WHERE `id`={}".format(catId, dbresult2['id'])
                    elif ttype == 2:
                        updateSql = "UPDATE `maoyan_movie_related` SET `source_id`='{}' WHERE `movie_id`={}".format(sourceId, dbresult['id'])
                    elif ttype == 3:
                        updateSql = "UPDATE `maoyan_movie_related` SET `year_id`='{}' WHERE `movie_id`={}".format(yearId, dbresult['id'])

                    else:
                        updateSql = ""

                    print(updateSql)

                    if not updateSql:
                        continue

                    result = Tools.dbUpdate(db, updateSql)
                    if result:
                        print('线程-%s 电影关系：%s 修改成功\r\n' % (thredname, title))
                    else:
                        print('线程-%s 电影关系：%s 修改失败\r\n' % (thredname, title))

                time.sleep(0.1)




if __name__ == '__main__':

    r = redis.Redis(**redisConf)
    # 创建队列
    workQueue = queue.Queue()

    # 启动线程
    for i in range(3):
        threads = threading.Thread(target=storage, args=(i, workQueue))
        threads.setDaemon(True)
        threads.start()
        # threads.join()

    print('消息监听线程启动...')
    tnum = 0
    while True:
        try:
            tmpdata = r.rpop('maoyan_movie_type')
            if not tmpdata:
                time.sleep(20)
                continue

            print('获取数据为：' + tmpdata)
            tmpdata = json.loads(tmpdata)
            workQueue.put(tmpdata)
            time.sleep(0.1)

            tnum = tnum + 1
            if tnum == 100:
                tnum = 0
                print('每100条休眠10秒')
                time.sleep(10)
                continue
        except Exception as e:
            print('获取数据错误：' + str(e))
            continue

