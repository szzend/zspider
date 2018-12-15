"""
"""
import abc
from functools import partial
from collections import defaultdict
from itertools import chain
from numbers import Integral

__all__=['ItemPipe','PipeLine']

class ItemPipe(abc.ABC):
    """ """
    @abc.abstractmethod
    def process(self, item, spider):
        """
        item为获取到的item实例
        spider为爬取到此item的Spider实例
        """

    def __str__(self):
        return f' <object:[{self.__class__.__name__} ]>'


class PipeLine:
    line = {}

    @classmethod
    def wraps(cls, target):
        instance = target()
        rank = cls.__args['rank']
        if cls.__args['is_global']:
            key = 'backend' if cls.__args['is_backend'] else 'frontend'
        else:
            key = cls.__args['spider'].__name__

        if key in cls.line:
            pass
        else:
            cls.line[key] = defaultdict(list)
        cls.line[key][rank].append(instance)

    @classmethod
    def register_global(cls, is_backend: bool = True, rank: int = 0,):
        """
        注册全局itempipe
        is_backend 指示是在专属itempipe处理完之后或之前进行全局处理
        rank为执行顺序,0为最前,数字越大越后.如果数字相同则依照定义先后顺序执行.
        默认为True在之后处理
        """
        return cls._register(rank, None, True, is_backend)

    @classmethod
    def register(cls, spider_class, rank: int = 0):
        """
        注册Spider专属itempipe
        spider_class为Spider类,itempipe只处理此Spider获取的数据
        rank为执行顺序,0为最前,数字越大越后.如果数字相同则依照定义先后顺序执行.
        """
        return cls._register(rank, spider_class, False, None)

    @classmethod
    def _register(cls, rank, spider, is_global, is_backend):
        """
        rank为执行顺序,0为最前,数字越大越后.如果数字相同则依照定义先后顺序执行.
        is_global指示是否为全局itempipe,如果非全局则须指定spider类,
        is_backend指示全局itempipe是否排在专属之后
        """
        s = 1 if spider else 0
        i = 1 if is_global else 0
        if not s ^ i:
            raise ValueError(
                f" check 'spider' or 'is_global',something is wrong.")
        if not isinstance(rank, Integral) or rank < 0:
            raise ValueError(f" 'rank' must be int and greater than zero.")
        cls.__args = {'rank': rank, 'spider': spider,
                      'is_global': is_global, 'is_backend': is_backend}
        return cls.wraps

    @classmethod
    async def call(cls, item, spider):
        """
        spider为实例
        """
        frontend = cls.line.get('frontend', {})
        backend = cls.line.get('backend', {})
        _spider = cls.line.get(spider.__class__.__name__, {})
        frontend = chain(*[frontend[k] for k in sorted(frontend.keys())])
        backend = chain(*[backend[k] for k in sorted(backend.keys())])
        _spider = chain(*[_spider[k] for k in sorted(_spider.keys())])
        total = chain(frontend, _spider, backend)
        for pipe in total:
            r=await pipe.process(item,spider)
            if r:
                item=r
            else:
                break