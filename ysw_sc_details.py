# coding=utf-8
import os
import re
import threading
import time
import redis
import requests
import platform
import datetime
import psycopg2
import traceback
from lxml import etree

# 连接Redis数据库
if "Win" in platform.system():
    RedisAddress = '127.0.0.1'
    RedisPort = 6379
    RedisDb = 0
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, db=RedisDb, encoding='utf-8', decode_responses=True)
    conn = psycopg2.connect(database="postgres", host='127.0.0.1', user='postgres', password='sw@12345', port='5432')
else:
    RedisAddress = '*.*.*.*'
    RedisPort = 6379
    RedisDb = 8
    RedisPassword = '*****'
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, password=RedisPassword, db=RedisDb, encoding='utf-8', decode_responses=True)
    conn = psycopg2.connect(database="crawler", host='*.*.*.*', user='root', password='*****', port='8635')

class YingShangwang(object):
    def __init__(self):
        self.crawlenum = 0
        self.error_info = None
        self.thread_num = 5
        self.errornum = 0
        self.errornum1 = 0
        self.headers = {

            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Cookie": "_uab_collina=159582189708767015709371; UM_distinctid=1738ed825bc726-0417dc653033fa-4353760-1fa400-1738ed825bd3e7; CNZZDATA1277846263=773642591-1595828296-http%253A%252F%252Fwww.winshangdata.com%252F%7C1607999656; Hm_lvt_f48055ef4cefec1b8213086004a7b78d=1607929668; Hm_lpvt_f48055ef4cefec1b8213086004a7b78d=1608622049; JSESSIONID=B598C7FD622DF813FC05D0909157FA60",
            "Host": "www.winshangdata.com",
            "Upgrade-Insecure-Requests": "1",
            "If-None-Match": '"a2199-vFblO8ICnZB/SOcaQzAEoBep8gE"',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"

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
            url='http://http.tiqu.qingjuhe.cn/getip?num=1&type=1&pack=***&port=1&lb=1&pb=4&regions=')

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
            detailsurl = 'http://www.winshangdata.com/projectDetail?projectId={}'
            projectId = redis_conn.lpop('projectIdlist')
            if not projectId:
                self.errornum1 += 1
                print('列表为空：', self.errornum)
                time.sleep(2)
                if self.errornum1 > 10:
                    print('任务结束')
                    print('结束时间：', time.asctime())
                    os._exit(1)
                continue
            else:
                time.sleep(0.8)
                self.errornum = 0
                self.errornum1 = 0
                detailsurl = detailsurl.format(str(projectId).replace('"', ''))
                print(detailsurl)

                proxies = {"https": "https://" + str(self.IP_list[str(thread_num)]),
                           "http": "http://" + str(self.IP_list[str(thread_num)])}
                try:
                    datajson = {}
                    res = requests.get(url=detailsurl, headers=self.headers, proxies=proxies, timeout=3)
                    reshtml = etree.HTML(res.text)
                    # 项目状态
                    try:
                        datajson['xiangmustatus'] = reshtml.xpath('//div[@class="clearfix border-b"]/div[1]/div[@class="detail-three-tit"]/text()')[0]
                    except:
                        datajson['xiangmustatus'] = ''
                    # 招商状态
                    try:
                        datajson['zhaoshangstatus'] = reshtml.xpath('//div[@class="clearfix border-b"]/div[2]/div[@class="detail-three-tit"]/text()')[0]
                    except:
                        datajson['zhaoshangstatus'] = ''
                    # 商业楼层
                    try:
                        datajson['floor'] = reshtml.xpath('//ul[@class="detail-option border-b"]/li[4]/span[2]/text()')[0]
                    except:
                        datajson['floor'] = ''
                    # 所在城市
                    try:
                        datajson['city'] = reshtml.xpath('//ul[@class="detail-option border-b"]/li[5]/span[2]/text()')[0]
                    except:
                        datajson['city'] = ''
                    # 项目地址
                    try:
                        datajson['address'] = reshtml.xpath('//ul[@class="detail-option border-b"]/li[6]/span[2]/text()')[0]
                    except:
                        datajson['address'] = ''
                    # 产品线项目
                    try:
                        datajson['isproject'] = reshtml.xpath('//ul[@class="detail-option border-b"]/li[7]/span[2]/text()')[0]
                    except:
                        datajson['isproject'] = ''
                    try:
                        Project_introduction = reshtml.xpath('//div[@class="detail-richtext"]//p/text()')
                    except:
                        Project_introduction = ''
                    datajson['project_introduction'] = ''.join(Project_introduction).strip().replace(' ', '').replace("'", '’').replace('\u3000', '').replace("'", '’').replace('"', '”')

                    jsondata = reshtml.xpath('//script[1]/text()')
                    try:
                        kaifashang = re.findall('kaiFaShang:"(.*?)",is', str(jsondata))[0]
                    except:
                        kaifashang = ''
                    datajson['kaifashang'] = kaifashang
                    try:
                        gd_lng = re.findall('gd_Lng:(.*?),gd_Lat', str(jsondata))[0]
                    except:
                        gd_lng = ''
                    datajson['gd_lng'] = gd_lng

                    try:
                        gd_lat = re.findall('gd_Lat:(.*?),indoor', str(jsondata))[0]
                    except:
                        gd_lat = ''
                    datajson['gd_lat'] = gd_lat

                    datajson['url'] = detailsurl
                    datajson['projectId'] = projectId
                    datajson['source'] = '赢商网'
                    datajson['collection_date'] = str(datetime.datetime.now())
                    print('当前品牌ID：', datajson['projectId'])
                    # print(datajson)
                    self.insert_data(datajson)


                except:
                    print('---异常---')
                    redis_conn.rpush('projectIdlist', projectId)
                    traceback.print_exc()
                    print('---切换IP---')
                    self.IP_list[str(thread_num)] = self.proxy_ip
                    self.errornum += 1
                    if self.errornum > 10:
                        os._exit(1)
                    continue

    def insert_data(self, datadict):
        self.crawlenum += 1
        # 存储数据
        sql = "update scrapy_ysw_mall_1221 set xiangmustatus = '{}', zhaoshangstatus = '{}', floor = '{}', city = '{}', address = '{}', isproject = '{}', project_introduction = '{}', kaifashang = '{}', gd_lng = '{}', gd_lat = '{}', url = '{}', source = '{}', collection_date = '{}' where projectId = '{}'".format(datadict['xiangmustatus'],datadict['zhaoshangstatus'],datadict['floor'],datadict['city'],datadict['address'],datadict['isproject'],datadict['project_introduction'],datadict['kaifashang'],datadict['gd_lng'],datadict['gd_lat'],datadict['url'],datadict['source'],datadict['collection_date'],datadict['projectId'])
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