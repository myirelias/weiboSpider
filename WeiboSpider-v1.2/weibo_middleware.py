# !/usr/bin/env python
# coding=UTF-8
# 中间件模块
from hashlib import md5
from rediscluster import StrictRedisCluster


class RedisMiddleware(object):
    """
    任务管理器，负责任务相关操作，如校验是否新增，读取已抓取任务文本
    """

    def __init__(self, redis_params):
        self.redis_cli = StrictRedisCluster(startup_nodes=redis_params.get('startup_nodes', ''),
                                            password=redis_params.get('password', ''))
        self.bloom_filter = BloomFilter(self.redis_cli, blockNum=5,
                                        key='bloomfilter_weibo')  # url的过滤器，分6个块存,内存空间默认512M

    def redis_del(self, key=None):
        """
        删除redis对应的键
        目前用在循环抓取时候，清空列表url，
        列表url每次循环只抓取一遍，直至下次循环
        :return:
        """
        if not key:
            return
        res = self.redis_cli.delete(key)
        return res

    def redis_rpush(self, name, data):
        """
        推入数据到redis指定任务列表中
        rpush,将新的数据放在最后面
        :return:
        """

        try:
            if isinstance(data, list):
                for each in data:
                    self.redis_cli.rpush(name, each)
            else:
                self.redis_cli.lpush(name, data)
        except:
            return

    def redis_lpush(self, name, data):
        """
        推入数据到redis指定任务列表中
        lpush,将新的数据放在最前面
        :return:
        """

        try:
            if isinstance(data, list):
                for each in data:
                    self.redis_cli.lpush(name, each)
            else:
                self.redis_cli.lpush(name, data)
        except:
            return

    def redis_rpop(self, name):
        """
        从指定任务列表中获取数据
        rpop,从最后取
        :return:
        """
        try:
            res = self.redis_cli.rpop(name)
            return res
        except:
            return

    def redis_lpop(self, name):
        """
        从指定任务列表中获取数据
        lpop,从头部取
        :return:
        """
        try:
            res = self.redis_cli.lpop(name)
            return res
        except:
            return

    def redis_brpop(self, name, timeout=1):
        """
        从指定任务列表中获取数据
        brpop,阻塞，从最后取
        :return:
        """
        try:
            unuse, res = self.redis_cli.brpop(name, timeout=timeout)
            return res
        except Exception as e:
            print(e)
            return

    def redis_query(self, name):
        """
        查询指定任务列表中数据
        :param name:
        :return:
        """
        try:
            res = self.redis_cli.llen(name)
            return res
        except:
            return

    def redis_sadd(self, name, data):
        """
        集合中插入数据
        :return:
        """
        try:
            if isinstance(data, list) or isinstance(data, set):
                for each in data:
                    self.redis_cli.sadd(name, each)
            else:
                self.redis_cli.sadd(name, data)
        except:
            return

    def redis_sismember(self, name, data):
        """
        校验元素是否存在于集合中
        :return:
        """
        return self.redis_cli.sismember(name, data)

    def redis_scard(self, name):
        """
        返回集合成员个数
        :return:
        """
        return int(self.redis_cli.scard(name))

    def redis_spop(self, name):
        """
        获取集合中的随机一个元素
        :param name:
        :return:
        """
        return self.redis_cli.spop(name)

    def redis_srem(self, name,data):
        """
        移除指定成员
        :param name:
        :param data:
        :return:
        """
        self.redis_cli.srem(name, data)



class SimpleHash(object):
    """
    简单的hash算法
    """

    def __init__(self, cap, seed):
        self.cap = cap

        self.seed = seed

    def hash(self, value):
        ret = 0

        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])

        return (self.cap - 1) & ret


class BloomFilter(object):
    def __init__(self, redis_cli, blockNum=1, key='bloomfilter_pub', bit_size=1 << 32):

        """

        :param host: the host of Redis

        :param port: the port of Redis

        :param db: witch db in Redis

        :param blockNum: one blockNum for about 90,000,000; if you have more strings for filtering, increase it.

        :param key: the key's name in Redis

        """

        self.server = redis_cli

        self.bit_size = bit_size  # Redis的String类型最大容量为512M

        # self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.seeds = [5, 11, 31]

        self.key = key

        self.blockNum = blockNum

        self.hashfunc = []

        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    def isContains(self, str_input):

        if not str_input:
            return False

        m5 = md5()

        m5.update(str_input.encode('utf-8'))

        str_input = m5.hexdigest()

        ret = True

        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)

        for f in self.hashfunc:
            loc = f.hash(str_input)

            ret = ret & self.server.getbit(name, loc)

        return ret

    def insert(self, str_input):

        m5 = md5()

        m5.update(str_input.encode('utf-8'))

        str_input = m5.hexdigest()

        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)

        for f in self.hashfunc:
            loc = f.hash(str_input)

            self.server.setbit(name, loc, 1)
