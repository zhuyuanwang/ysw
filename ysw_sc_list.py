# coding=utf-8
import json
import os
import threading
import time
import redis
import requests
import platform
import psycopg2
import traceback

# 连接Redis数据库
if "Win" in platform.system():
    RedisAddress = '127.0.0.1'
    RedisPort = 6379
    RedisDb = 0
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, db=RedisDb, encoding='utf-8', decode_responses=True)
    conn = psycopg2.connect(database="postgres", host='127.0.0.1', user='postgres', password='*****', port='5432')
else:
    RedisAddress = '10.101.0.239'
    RedisPort = 6379
    RedisDb = 8
    RedisPassword = '*****'
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, password=RedisPassword, db=RedisDb, encoding='utf-8', decode_responses=True)
    conn = psycopg2.connect(database="crawler", host='*.*.*.*', user='root', password='*****', port='8635')

class YingShangwang(object):
    def __init__(self):
        self.crawlenum = 0
        self.error_info = None
        self.thread_num = 1
        self.errornum = 0
        self.errornum1 = 0
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "appType": "bigdata",
            "Authorization": "",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Cookie": "_uab_collina=159582189708767015709371; UM_distinctid=1738ed825bc726-0417dc653033fa-4353760-1fa400-1738ed825bd3e7; CNZZDATA1277846263=773642591-1595828296-http%253A%252F%252Fwww.winshangdata.com%252F%7C1607999656; Hm_lvt_f48055ef4cefec1b8213086004a7b78d=1607929668; JSESSIONID=FF0658C08812393465A0D58B940DF621; Hm_lpvt_f48055ef4cefec1b8213086004a7b78d=1608533123",
            "Host": "www.winshangdata.com",
            "Origin": "http://www.winshangdata.com",
            "platform": "pc",
            "Referer": "http://www.winshangdata.com/projectList",
            "token": "",
            "uid": "",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
            "uuid": "123456"
        }
        # 搭建IP池
        self.IP_list = {}
        for i in range(self.thread_num):
            time.sleep(2)
            self.IP_list[str(i)] = self.proxy_ip
            print('当前IP：', self.IP_list[str(i)])
        print("IP列表：", self.IP_list)

    @property
    def proxy_ip(self):
        return self.get_proxy(
            url='http://http.tiqu.qingjuhe.cn/getip?num=1&type=1&pack=*****&port=1&lb=1&pb=4&regions=')

    @staticmethod
    def get_proxy(url=None):
        """请求代理"""
        if not url:
            return None
        while True:
            time.sleep(2)
            try:
                r = requests.get(url)
            except Exception as e:
                print(e)
                time.sleep(2)
                continue
            print(r.text.strip())
            if "msg" in r.text:
                print('ip error：', r.text)
                time.sleep(10)
                continue

            ip_port = r.text.strip().replace('\r\n', '')
            return ip_port

    def ysw(self, thread_num):
        while True:
            listurl = 'http://www.winshangdata.com/wsapi/project/list3_4'
            page = redis_conn.lpop('pagenumList')
            if not page:
                self.errornum1 += 1
                print('列表为空：', self.errornum1)
                time.sleep(2)
                if self.errornum1 > 10:
                    print('任务结束')
                    print('结束时间：', time.asctime())
                    os._exit(1)
                continue
            else:
                time.sleep(1)
                self.errornum = 0
                self.errornum1 = 0
                # 总页数为471页

                data = {"pageNum":page, "orderBy": "1","pageSize": 60,"zsxq_yt1": "","zsxq_yt2": "","qy_p": "","qy_c": "","qy_a": "","xmzt": "","key": "","wuyelx": "","isHaveLink": "","ifdporyt": ""}

                proxies = {"https": "https://" + str(self.IP_list[str(thread_num)]),
                           "http": "http://" + str(self.IP_list[str(thread_num)])}
                try:
                    res = requests.post(url=listurl, headers=self.headers, data=json.dumps(data), proxies=proxies, timeout=3)
                    resjson = json.loads(res.text)
                    print('第{}页'.format(str(page)))
                    reslist = resjson['data']['list']
                    datajson = {}
                    for resdata in reslist:
                        # 详情页id
                        try:
                            datajson['projectId'] = resdata['projectId']
                        except:
                            datajson['projectId'] = ''
                        # 商场名称
                        try:
                            datajson['projectName'] = resdata['projectName']
                        except:
                            datajson['projectName'] = ''
                        # 物业类型
                        try:
                            datajson['wuYeLx'] = resdata['wuYeLx']
                        except:
                            datajson['wuYeLx'] = ''
                        # 图片
                        try:
                            datajson['projectPic'] = resdata['projectPic']
                        except:
                            datajson['projectPic'] = ''
                        # 开业时间
                        try:
                            datajson['kaiYeShiJian'] = resdata['kaiYeShiJian']
                        except:
                            datajson['kaiYeShiJian'] = ''
                        # 面积
                        try:
                            datajson['shangYeMianji'] = resdata['shangYeMianji']
                        except:
                            datajson['shangYeMianji'] = ''
                        # 招商详情
                        try:
                            datajson['zhaoShangXQ'] = resdata['zhaoShangXQ']
                        except:
                            datajson['zhaoShangXQ'] = ''
                        #

                        # 将品牌id去重后存入redis列表，便于后续对品牌详情页的抓取
                        if redis_conn.sadd('projectId_set', datajson['projectId']):
                            redis_conn.lpush('projectIdlist', datajson['projectId'])
                            print('当前品牌名：', datajson['projectName'])
                            # 将列表页获取的相关字段存入数据库表
                            self.insert_data(datajson)
                        else:
                            print('{}：已存在'.format(datajson['projectName']))
                # 出现异常报错时更换ip并将访问失败的页码重新push到任务列表
                except:
                    print('---异常---')
                    redis_conn.rpush('pagenumList', page)
                    traceback.print_exc()
                    print('---切换IP---')
                    self.IP_list[str(thread_num)] = self.proxy_ip
                    self.errornum += 1
                    if self.errornum > 10:
                        os._exit(1)
                    continue

    def insert_data(self, datadict):
        self.crawlenum += 1
        valuesdict = [i for i in datadict]
        keysdict = [datadict[i] for i in datadict]
        valuesdictdata = (','.join(valuesdict))
        # 存储数据
        sql = 'insert into scrapy_ysw_mall_1221 ({}) values ({})'.format(valuesdictdata, str(keysdict)[1:-1])
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            print('存储成功，第{}条数据'.format(str(self.crawlenum)))
            # time.sleep(1)
        except Exception as e:
            conn.rollback()
            print('error_info:{}'.format(e))

    def checkthread(self, initThreadsName):
        while True:
            # print('监控程序持续运行中')
            newThreadsName = []
            for i in threading.enumerate():
                # TODO 记录正在运行的线程
                newThreadsName.append(i.getName())

            # TODO 判断有没有线程中途挂掉 如果有就重启线程
            for oldname in initThreadsName:
                if oldname in newThreadsName:
                    pass
                else:
                    print(oldname)
                    thread = threading.Thread(target=self.ysw, args=oldname)
                    thread.setName(oldname)
                    thread.start()
                    print('重新启动了线程：{}'.format(oldname))
            time.sleep(5)

    def thread_start(self):
        thread_list = []
        init_thread_name = []  # TODO 记录线程名
        for i in range(self.thread_num):
            thread = threading.Thread(target=self.ysw, args=str(i))
            # TODO 给线程赋值
            thread.setName(str(i))
            thread_list.append(thread)

        for thread in thread_list:
            thread.start()
            time.sleep(0.1)
        # TODO 获取初始化的线程对象
        init = threading.enumerate()
        for i in init:
            # TODO 保存初始化线程名字
            init_thread_name.append(i.getName())
        time.sleep(1)
        thread_pro = threading.Thread(target=self.checkthread, args=(init_thread_name,))
        thread_pro.start()

        for thread in thread_list:
            thread.join()

        time.sleep(1)
        self.thread_start()
        thread_pro.join()

if __name__ == "__main__":
    print('脚本启动：', time.asctime())
    ysw = YingShangwang()
    ysw.thread_start()