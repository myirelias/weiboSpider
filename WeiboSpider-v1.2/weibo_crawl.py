# coding=utf-8
'''抓取模块'''

import requests


class Crawl(object):
    """
    抓取模块，对提供的url进行抓取并返回获取的页面content
    crawl_by_get 使用get请求方式获取页面内容
    crawl_by_post 使用post请求方式获取页面内容
    """

    def __init__(self):
        """
        初始化方法
        """
        self.session = requests.Session()

    def crawl_by_get(self, url, **kw):
        """
        使用requests使用get方法获取页面content
        :param url:
        :param kw:
        :return:
        """
        retry = kw.get('retry', 5)
        res = None
        while retry:
            retry -= 1
            try:
                response = self.session.get(url, headers=kw.get('headers'), params=kw.get('params'),
                                            proxies=kw.get('proxies'), timeout=kw.get('timeout', 30))
                if response.status_code in [200, 404]:
                    res = response.content.decode(kw.get('pagecode', 'utf-8'))
                    break
                else:
                    continue
            except UnicodeError as e:
                print(e)
                kw['pagecode'] = 'gbk'
                continue
            except Exception as e:
                print(e)
                continue

        return res

    def crawl_by_post(self, url, **kw):
        """
        使用requests使用post方法获取页面content
        :param url:
        :param kw:
        :return:
        """
        retry = kw.get('retry', 5)
        res = None
        while retry:
            retry -= 1
            try:
                response = self.session.post(url, headers=kw.get('headers'), data=kw.get('data'),
                                             proxies=kw.get('proxies'), timeout=kw.get('timeout', 30))
                if response.status_code in [200, 404]:
                    res = response.content.decode(kw.get('pagecode', 'utf-8'))
                    break
                else:
                    continue
            except UnicodeError:
                kw['pagecode'] = 'gbk'
                continue
            except Exception:
                continue

        return res
