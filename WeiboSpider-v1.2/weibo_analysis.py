# coding=utf-8
'''解析模块'''

import re
from lxml import etree


class Analysis(object):
    """
    解析模块，将页面content解析出需要的模块
    analysis_by_xpath 使用xpath规则解析页面
    analysis_by_regex 使用正则表达式解析页面
    """
    def __init__(self):
        pass

    def analysis_by_xpath(self, content, xpahter, *kw):
        """
        解析方法，将指定页面按照指定xpath规则进行解析并返回解析结果
        :param content:
        :param xpahter:
        :param kw:
        :return:
        """
        try:
            seletor = etree.HTML(content)
        except:
            seletor = content

        try:
            if isinstance(xpahter, dict):
                res = {}
                for eachkey in xpahter.keys():
                    res[eachkey] = ''.join(seletor.xpath(xpahter[eachkey])).replace('\n', '').replace('\r', '')\
                        .replace('\t', '').replace(' ', '')
            elif isinstance(xpahter, str):
                res = seletor.xpath(xpahter)
            else:
                return
        except:
            return

        return res

    def analysis_by_regex(self, content, regexer):
        """
        使用正则表达式解析页面
        :param content:
        :param regexer:
        :return:
        """
        pass
