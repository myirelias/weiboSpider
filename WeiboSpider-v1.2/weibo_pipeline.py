# coding=utf-8
'''存储模块'''

import os
import pickle
try:
    from pymongo import MongoClient
except:
    pass


class Pipeline(object):
    """
    存储模块，负责对数据进行持久化操作或读取数据操作
    pipe_mongo_save 存储数据到mongodb
    pipe_mongo_load 读取数据到mongodb
    pipe_txt_save 存储数据到txt文本
    pipe_txt_load 从txt文本加载数据
    pipe_pickle_save 将内存中的数据持久化到硬盘中
    pipe_pickle_load 将硬盘中的数据加载并置于内存
    """

    def __init__(self, mongo_host, mongo_port):
        self.conn = MongoClient(host=mongo_host, port=mongo_port)
        self.path = os.path.join(os.path.abspath('DATA'), '{}')
        self._pipe_create_data()

    @staticmethod
    def _pipe_create_data():
        """
        创建DATA文件夹
        :return:
        """
        if not os.path.exists('DATA'):
            os.makedirs('DATA')

    def pipe_remove_file(self, filename):
        """
        创建DATA文件夹
        :return:
        """
        if os.path.exists(self.path.format(filename)):
            os.remove(self.path.format(filename))

    def pipe_mongo_save(self, data, **kw):
        """
        使用mongodb存储数据
        :param data:
        :param kw:
        :return:
        """
        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, list) or isinstance(data, set):
            pass
        else:
            print('data format error, must be dict or dict-list')

        try:
            db = self.conn[kw.get('dbname', 'default')]
            col = db[kw.get('colname', 'default')]
            col.insert_many(data)  # insert_many
        except Exception as e:
            print('[error]by save mongo', e)

    def pipe_mongo_load(self, **kw):
        """
        加载mongodb中的数据
        :param kw:
        :return:
        """
        if kw.get('value'):
            value = kw['value']
        else:
            value = {}
        if kw.get('detail'):
            detail = kw['detail']  # 指定展示的字段
        else:
            detail = ''
        try:
            db = self.conn[kw.get('dbname', 'default')]
            col = db[kw.get('colname', 'default')]
            if detail:
                res = col.find(value, {'{}'.format(detail): 1, '_id': 0})
            else:
                res = col.find(value, {'_id': 0})
            return list(res)
        except Exception as e:
            print('[error]by save mongo', e)

    def pipe_mongo_update(self, data, **kw):
        """
        更新或插入数据
        :param data:
        :param kw:
        :return:
        """
        tourist_id = data['tourist_id']
        try:
            db = self.conn[kw.get('dbname', 'default')]
            col = db[kw.get('colname', 'default')]
            res = col.update({'tourist_id': tourist_id}, {'$set': data})
            # 更新数据失败，不存在该数据，则插入数据
            if not res.get('updatedExisting'):
                col.insert(data)
        except Exception as e:
            print('[error]by save mongo', e)

    def pipe_mongo_query(self, **kw):
        """
        查询数据
        :param kw:
        :return:
        """
        value = kw.get('value')
        if not value:
            value = {}
        try:
            db = self.conn[kw.get('dbname', 'default')]
            col = db[kw.get('colname', 'default')]
            res = col.find(value).count()
            return res
        except Exception as e:
            print('[error]by save mongo', e)

    def pipe_txt_save(self, data, **kw):
        """
        将数据存储到txt文本中
        :param data:
        :param kw:
        :return:
        """
        with open(self.path.format(kw.get('filename', 'default.txt')), kw.get('savetype', 'a'), encoding='utf-8') as f:
            if isinstance(data, list) or isinstance(data, set):
                for each in data:
                    f.write(each + '\n')
            elif data == '':
                pass
            elif isinstance(data, str):
                f.write(data + '\n')

    def pipe_txt_load(self, **kw):
        """
        从txt文本中加载数据
        :param kw:
        :return:
        """
        try:
            with open(self.path.format(kw.get('filename', 'default.txt')), kw.get('loadtype', 'r'), encoding='utf-8') as f:
                return f.read()
        except:
            return

    def pipe_pickle_save(self, data, **kw):
        """
        从内存持久化到硬盘中
        :param data:
        :param kw:
        :return:
        """
        with open(self.path.format(kw.get('filename', 'default.txt')), 'wb') as f:
            pickle.dump(data, f)

    def pipe_pickle_load(self, **kw):
        """
        将硬盘中持久化的数据加载到内存
        :return:
        """
        try:
            with open(self.path.format(kw.get('filename', 'default.txt')), 'rb') as f:
                return pickle.load(f)
        except:
            return

    def pipe_hdfs_write(self, data, hdfs_service, path, writetype='wb'):
        """
        将数据直接写到hdfs中
        :param writetype: 写入方式
        :param path: 存储路径
        :param data:待存储数据
        :param hdfs_service: hdfs服务
        :return:
        """
        with hdfs_service.open(path, writetype) as f:
            if isinstance(data, list) or isinstance(data, set):
                for each in data:
                    f.write(each + '\n')
            elif data == '':
                pass
            elif isinstance(data, str):
                f.write(data + '\n')

    def pipe_hdfs_read(self, hdfs_service, path, readtype='rb'):
        """
        从hdfs加载数据
        :param readtype: 读取方式
        :param hdfs_service: hdfs服务
        :param path: 存储路径
        :return:
        """
        try:
            with hdfs_service.open(path, readtype) as f:
                return f.read().decode('utf-8')
        except:
            return []

    def pipe_hdfs_exists(self, hdfs_service, path):
        """
        校验是否存在该文件
        :param hdfs_service:
        :param path:
        :return:
        """
        return hdfs_service.exists(path)

    def pipe_hdfs_mv(self, hdfs_service, oldpath, newpath):
        """
        将文件从oldpath移动到newpath
        :param hdfs_service:
        :param oldpath:
        :param newpath:
        :return:
        """
        try:
            hdfs_service.mv(oldpath, newpath)
        except Exception as e:
            print('move faile', e)

    def pipe_data_trans(self, datalist):
        """
        数据转换，主要针对hdfs上的数据进行字节的转换
        :param datalist: 待转换数据
        :return:
        """
        newdata = []
        if isinstance(datalist, list) or isinstance(datalist, set):
            for each in datalist:
                try:
                    newdata.append(each.decode('utf-8'))
                except:
                    continue
            return newdata
        elif isinstance(datalist, str):
            try:
                return datalist.decode('utf-8')
            except:
                return



