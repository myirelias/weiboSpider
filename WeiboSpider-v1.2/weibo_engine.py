# !/usr/bin/env python
# coding=UTF-8
"""
sina weibo
2019.07.11
---2019.09.03---
1. 新增所在地字段
2. 新增获取转发内容字段
3. 新增获取当前点赞 转发 评论数字段
4. bloomfilter扩容
5. 将任务队列放到redis-v1.1版本直接放在内存中
"""

import time
import datetime
import queue
import threading
from faker import Faker
from copy import deepcopy
import json
import re
from weibo_crawl import Crawl
from weibo_analysis import Analysis
from weibo_pipeline import Pipeline
from weibo_middleware import RedisMiddleware
# import config as setting
import logging
from configparser import ConfigParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s---%(lineno)s----%(name)s: %(message)s",
    filename="weiboSpider.log",
    filemode="a", )

config_file = './setting.conf'


class Engine(object):
    """
    引擎模块 负责调度
    """

    def __init__(self):
        self.config = self.__engine_load_config()
        self.crawl = Crawl()
        self.analysis = Analysis()
        mongo_host = self.config.get('MONGODB', 'MONGO_HOST')
        mongo_port = int(self.config.get('MONGODB', 'MONGO_PORT'))
        self.pipe = Pipeline(mongo_host, mongo_port)
        redis_params = self.config.get('REDIS', 'REDIS_PARAMS')
        self.redis_tool = RedisMiddleware(eval(redis_params))
        # self.user_queue = queue.Queue()  # 用户列表队列
        # self.publish_queue = queue.Queue()  # 用户发布队列
        self.user_queue = 'weibo_user:task'  # 用户任务队列
        self.publish_queue = 'weibo_publish:task'  # 详情任务队列
        self.crawled_queue = 'weibo_crawled:dupefilter'  # 所有已获取id

    def _engine_weibo_list(self):
        """
        获取所有微博用户id等列表数据
        :return:
        """
        while True:
            each_id_redis = self.redis_tool.redis_brpop(self.user_queue, timeout=0)  # 当前待抓取id
            each_id = str(each_id_redis, encoding='utf-8')
            # print(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]{threading.current_thread().name} 开始爬id-{each_id}')
            # 先抓基本信息，主要是
            for p_name, p_id in eval(self.config.get('PARAMS', 'PARAMS_CONTAINERID')).items():  # 粉丝和关注两个模块内容
                current_headers = eval(self.config.get('HEADERS', 'headers'))
                params_f = eval(self.config.get('PARAMS', 'PARAMS_FANS'))  # 粉丝参数 关注参数
                params_f['containerid'] = p_id.format(each_id)  # 粉丝、关注该内容有差异
                pgup_id = 1  # 粉丝翻页为since_id 关注翻页为page
                while True:
                    time.sleep(1)
                    current_headers['User-Agent'] = self._eneing_user_agent()  # 每次请求使用不同的ua
                    current_headers['Proxy-Switch-Ip'] = 'yes'  # 切换代理IP
                    if pgup_id != 1:
                        params_f[p_name] = pgup_id  # 第一页不含此参数,第二页请求需要此参数
                    current_headers['Referer'] = self._engine_referer(self.config.get('API', 'API_REFERER'),
                                                                      params_f)
                    content = self.crawl.crawl_by_get(self.config.get('API', 'API_FANS_FOLLOWER'),
                                                      headers=current_headers,
                                                      proxies=self._engine_proxy_ip(), params=params_f)
                    try:
                        current_json = json.loads(content)
                    except:
                        continue
                    if current_json.get('ok') == 0:  # 没有内容的时候 ok=0 否则 ok=1
                        # test_save = f'[{datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")}]{each_id}抓取到{pgup_id}页-结束'
                        # self.pipe.pipe_txt_save(data=test_save, filename='test.log', savetype='a')  # 基本上是到251结束
                        break
                    try:
                        # 数据在json中该位置，因为第一页的时候会出现关注分类，从该列表中最后一个列表元素为个人用户数据
                        card_gourp = current_json.get('data').get('cards')[-1].get('card_group')
                        save_datas = self._engine_check_data(card_gourp)  # 数据校验，去重，存id，返回新增数据
                        if save_datas:
                            self.pipe.pipe_mongo_save(data=save_datas, dbname='db_sina_weibo',
                                                      colname='weibo_user_info')
                    except Exception as e:
                        # print(f'{e}')
                        logging.warning('数据获取异常{}'.format(e))
                        continue
                    pgup_id += 1

    def _engine_weibo_content(self):
        """
        获取微博详细内容
        :return:
        """
        # 未涉及到频率问题 相当于通爬
        while 1:
            # 手动阻塞
            if self.redis_tool.redis_scard(self.publish_queue) == 0:
                time.sleep(5)
                continue
            each_url_redis = self.redis_tool.redis_spop(self.publish_queue)
            each_url = str(each_url_redis, encoding='utf-8')
            # print(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]{threading.current_thread().name} 开始爬url-{each_url}')
            current_headers = eval(self.config.get('HEADERS', 'headers'))
            current_headers['Referer'] = each_url
            current_headers['Proxy-Switch-Ip'] = 'yes'
            current_headers['User-Agent'] = self._eneing_user_agent()
            params_list = each_url.split('?')[-1].split('&')
            params_dict = {
                'display': '0',  # 固定参数
                'retcode': '6102',  # 固定参数
                'type': 'uid',  # 固定参数
                'value': '5950061090',  # 用户id
                'containerid': '1076035950061090',  # 107603+用户id
                # 'page': '5',  # 页码 page>=2,第一页数据不含次参数
            }
            for each_params in params_list:
                params_dict[each_params.split('=')[0]] = each_params.split('=')[1]
            user_id = params_dict['uid']
            user_dict = self._engine_weibo_user(user_id, headers=current_headers)
            params_dict['value'] = user_id
            params_dict['containerid'] = '107603{}'.format(user_id)
            page = 1
            while True:
                time.sleep(0.5)
                if page != 1:
                    params_dict['page'] = page
                content = self.crawl.crawl_by_get(self.config.get('API', 'API_FANS_FOLLOWER'),
                                                  headers=current_headers,
                                                  proxies=self._engine_proxy_ip(), params=params_dict)
                try:
                    current_json = json.loads(content)
                except Exception as e:
                    logging.warning('获取json出错{}'.format(e))
                    continue
                if current_json.get('ok') == 0:  # 没有内容的时候 ok=0 否则 ok=1
                    break
                mylog_list = current_json.get('data', {}).get('cards', [])
                for eachlog in mylog_list:
                    mblog_id = eachlog.get('mblog', {}).get('id', '')  # 去重用
                    if not mblog_id:
                        continue
                    if self.redis_tool.bloom_filter.isContains(mblog_id):  # 使用bloom过滤器去重
                        continue
                    else:
                        self.redis_tool.bloom_filter.insert(mblog_id)
                    try:
                        curren_dict = {
                            'card_type': eachlog.get('card_type', ''),
                            'itemid': eachlog.get('itemid', ''),
                            'scheme': eachlog.get('scheme', ''),
                            'mblog_id': eachlog.get('mblog', {}).get('id', ''),

                            'mblog_source': eachlog.get('mblog', {}).get('source', ''),
                            'mblog_created_at': eachlog.get('mblog', {}).get('created_at', ''),
                            'pics': eachlog.get('mblog', {}).get('pics', []),
                            'user_id': user_id,
                            'source_url': each_url,
                            'reposts_count': eachlog.get('mblog', {}).get('reposts_count', 0),  # 转发数
                            'comments_count': eachlog.get('mblog', {}).get('comments_count', 0),  # 评论数
                            'attitudes_count': eachlog.get('mblog', {}).get('attitudes_count', 0),  # 点赞数
                            'crawl_date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        mblog_text = eachlog.get('mblog', {}).get('text', '')
                        retweet_text = eachlog.get('mblog', {}).get('retweeted_status', {}).get('text', '')  # 转发内容
                        retweet_id = eachlog.get('mblog', {}).get('retweeted_status', {}).get('id', '')  # 转发内容id
                        # 转发内容创建时间
                        retweet_date = eachlog.get('mblog', {}).get('retweeted_status', {}).get('created_at', '')
                        deal_text = self._eneign_deal_text(mblog_text)
                        deal_retweet = self._eneign_deal_text(retweet_text)
                        curren_dict['mblog_text'] = deal_text
                        curren_dict['retweet_text'] = deal_retweet
                        curren_dict['retweet_id'] = retweet_id
                        curren_dict['retweet_date'] = retweet_date
                        curren_dict.update(user_dict)
                        self.pipe.pipe_mongo_save(curren_dict, dbname='db_sina_weibo', colname='weibo_publish_content')
                    except Exception as e:
                        logging.warning('构建存储数据异常{}'.format(e))
                        continue
                page += 1

    def _engine_weibo_user(self, uid, **kw):
        """
        获取用户基础信息
        :param uid: 用户id
        :return:用户昵称，所在地及性别
        """
        params_site = {
            'containerid': '230283{}_-_INFO'.format(uid),
            'lfid': '230283{}'.format(uid),
            'luicode': '10000011',
            'title': '基本资料',
            'type': 'uid',
            'value': '{}'.format(uid),
        }
        content_site = self.crawl.crawl_by_get(self.config.get('API', 'API_FANS_FOLLOWER'), params=params_site,
                                               headers=kw.get('headers'), proxies=self._engine_proxy_ip())
        try:
            site_json = json.loads(content_site)
            info_1 = site_json.get('data', {}).get('cards', [])[0]
            info_2 = site_json.get('data', {}).get('cards', [])[1]
        except Exception as e:
            logging.warning('获取所在地json出错{}'.format(e))
            return {}
        user_dict = {}
        try:
            if info_1.get('card_group')[1].get('item_name', '') == '昵称':
                user_dict['nick'] = info_1.get('card_group', '')[1].get('item_content')
        except:
            ...
        try:
            if info_2.get('card_group')[1].get('item_name', '') == '性别':
                user_dict['gender'] = info_2.get('card_group', '')[1].get('item_content')
        except:
            ...
        try:
            if info_2.get('card_group')[2].get('item_name', '') == '所在地':
                user_dict['location'] = info_2.get('card_group', '')[2].get('item_content')
        except:
            ...
        return user_dict

    def _engine_check_data(self, checkdata):
        """
        提取id并去重
        返回去重结果
        :return:
        """
        check_res = []
        for eachdata in checkdata:
            try:
                current_id = eachdata.get('user').get('id')
                profile_url = eachdata.get('user').get('profile_url')
                eachdata['crawl_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                eachdata.pop('buttons')
                eachdata.pop('actionlog')
                # 校验是否已庄主
                if self.redis_tool.redis_sismember(self.crawled_queue, current_id):
                    continue
                else:
                    self.redis_tool.redis_sadd(self.crawled_queue, current_id)
                    self.redis_tool.redis_lpush(self.user_queue, current_id)
                    self.redis_tool.redis_sadd(self.publish_queue, profile_url)
                    check_res.append(eachdata)
            except:
                continue
        return check_res

    def _engine_publish_task(self):
        """
        任务分发
        可实现持续抓取持续发布，目前为一轮抓取完毕后再发布
        :return:
        """
        while 1:  # 循环发布任务
            # 当用户队列和详情队列都为空时，默认为上次抓取全部完毕
            # 删除当前已抓取任务去重队列
            if self.redis_tool.redis_query(self.user_queue) == 0 and \
                            self.redis_tool.redis_query(self.publish_queue) == 0:
                self.redis_tool.redis_del(self.crawled_queue)
            # 当仅有用户队列为空时候，删除起始任务
            elif self.redis_tool.redis_query(self.user_queue) == 0:
                self.redis_tool.redis_srem(self.user_queue, '2214257545')
            # 发布新的初始任务
            self.redis_tool.redis_lpush(self.user_queue, '2214257545')
            # if self.redis_tool.redis_query(self.publish_queue) == 0:  # 用户发布内容的队列
            #     user_list = self.pipe.pipe_mongo_load(dbname='db_sina_weibo', colname='col_user_info', detail='user')
            #     profile_urls = set(map(lambda x: x.get('user').get('profile_url'), user_list))
            #     for each_url in profile_urls:
            #         self.publish_queue.put(each_url)
            time.sleep(600)  # 每600秒检查一次队列中任务是否被消费完

    @staticmethod
    def _engine_referer(url, params):
        """
        构建Referer
        :return:
        """
        if not isinstance(params, dict):
            return
        cur_str = '{}?'.format(url)
        for k, v in params.items():
            cur_str += '{}={}&'.format(k, v)
        return cur_str[0:-1]

    @staticmethod
    def _eneign_deal_text(text):
        """
        对微博正文清洗
        :param text:
        :return:
        """
        try:
            pattern = re.compile(r'<.*?>', re.S)
            res = re.sub(pattern, '', text)
            clean_data = res.replace('\r', '').replace('\n', '').replace(' ', '').replace('\t', '')
        except:
            return '获取微博内容失败'
        return clean_data

    def _engine_proxy_ip(self):
        """
        使用代理ip
        :return: 代理ip
        """

        proxy_meta = "http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}".format(
            **eval(self.config.get('PROXY', 'PROXY_ACCOUNT')))
        proxies = {"http": proxy_meta,
                   "https": proxy_meta}

        return proxies

    @staticmethod
    def _eneing_user_agent():
        """
        返回随机ua
        :return:
        """
        fa = Faker()
        return fa.user_agent()

    @staticmethod
    def __engine_load_config():
        """
        加载配置文件
        :return:
        """
        conf = ConfigParser()
        conf.read(config_file, encoding='utf-8')
        return conf

    def excute(self):
        # self._engine_weibo_list()
        # self._engine_weibo_content()
        list_spider_count = int(self.config.get('THREAD_COUNT', 'LIST_SPIDER_COUNT'))
        content_spider_count = int(self.config.get('THREAD_COUNT', 'CONTENT_SPIDER_COUNT'))
        thd_task = threading.Thread(target=self._engine_publish_task)
        thd_task.start()
        thd_join = []
        for i in range(list_spider_count):
            thd_list = threading.Thread(target=self._engine_weibo_list)
            thd_list.start()
            thd_join.append(thd_list)
        for i in range(content_spider_count):
            thd_content = threading.Thread(target=self._engine_weibo_content)
            thd_content.start()
            thd_join.append(thd_content)
        for each in thd_join:
            each.join()


if __name__ == '__main__':
    start = time.time()
    proc = Engine()
    proc.excute()
    end = time.time()
    print('[{:.2f}s] script finish '.format(end - start))
