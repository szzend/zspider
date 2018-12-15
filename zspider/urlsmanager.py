"""
"""
import json
import logging
from asyncio import PriorityQueue, Queue
from collections import defaultdict, namedtuple
import itertools
from yarl import URL
from collections.abc import Iterable
Counter = namedtuple("Counter", "count put discarded done failed todo working keys sum min max",
                     defaults=(None, None, None, None, None, None, None, None, None, None, None))

__all__ = ['Counter', 'UrlsManager']


class UrlsManager:
    """
    内部记录每个url的运行信息
    并保存最终运行结果
    """
    class _counter:
        """
        内部计数的自定义类,
        维护一个namedtuple[Counter]
        """
        def __new__(cls, fields):
            cls.fields = fields._fields_defaults
            return super().__new__(cls)

        def __init__(self, fields: Counter):
            # _dict用于实际保存并计数
            f = fields._fields
            self._dict = dict(zip(f, [0 for i in range(len(f))]))

        def __setattr__(self, name, value):
            """所有的赋值操作都会调用"""
            # 阻止对_dict的直接赋值
            if (name == '_dict' and hasattr(self, '_dict') and isinstance(getattr(self, '_dict'), dict)):
                raise ValueError(f' Forbidden to modify attribute:[{name}]')
            if name == '_dict':  # 本实现将阻止除了更新计数之外的其它设值及增加属性,模拟了namedtuple抛出异常
                super().__setattr__(name, value)
            elif name in self._dict:
                self._dict[name] = value
            else:
                raise ValueError(f' Got unexpected field names:[{name}]')

        def __getattribute__(self, name):
            """
            __getattribute__在任何属性查找操作中都会调用(包含特殊属性),所以注意以下要super调用
            否则会陷入无限递归调用.
            __getattr__方法则是在本身及其类上查找不到才会调用
            """
            # 注意特殊属性及类自身属性.实际应用时应注意
            if name in super().__getattribute__('_dict'):
                return super().__getattribute__('_dict')[name]
            else:
                return super().__getattribute__(name)

        def __delattr__(self, name):
            """拦截了所有删除操作"""
            raise ValueError(f' Forbidden to delete attribute:[{name}]')

        def add(self, n: Counter):
            """
            使用数值累加计数器
            """
            for key in n._fields:
                v = getattr(n, key)
                if v:
                    self._dict[key] += v

        def update(self, n: Counter):
            v = None
            for key in n._fields:
                v = getattr(n, key)
                if v:
                    self._dict[key] = v

        @property
        def values(self):
            return Counter(**self._dict)

    def __init__(self, max_tried_times: int = 3):
        """
        参数:max_tried_times 为任务失败前最大尝试次数,需为大于0的整数
        """
        self.max_tried_times = max_tried_times
        # 以下Queue必须在主事件循环event loop确定后再建立，
        self.__queue_todo = None  # 保存待完成的任务
        # 其构造函数为Queue(maxsize=0, *, loop=None)，不指定loop时
        # 内部获取的event loop可能与主event loop不同，则join方法会产生runtime error
        self.__total_key_urls = defaultdict(int)  # 用于过滤url并保存处理次数,键为url,值为已处理次数
        self.__done_urls = set()  # 用于保存完成记录
        self.__failed_urls = set()  # 用于保存每次失败记录
        self.__working_urls = set()  # 用于保存正在处理的url
        self.__discarded_urls = set()  # 保存丢弃的url及丢弃次数
        self.__counter = self._counter(Counter)  # 维护内部处理计数器

    async def prepare(self)->bool:
        """需要控制时间的初始化或资源准备"""
        self.__queue_todo = PriorityQueue()
        return True

    async def put(self, url: str or URL)->Counter:
        """
        向UlrsManager发送1条url
        参数：url为字符串
        返回：Counter 指示此次添加的url处理方式
        此方法在内部对url有两种处理方式:put进任务队列或discard
        """
        url=URL(url)
        put = discarded = 0  # 传递给计数器_counter
        times = self.__total_key_urls[url]
        _todo = {url: rank for rank,url in self.__queue_todo._queue}  # 将优先队列中数据转换成字典形式
        # 如果达到尝试次数或者已在任务中则丢弃它
        if (times >= self.max_tried_times or url in itertools.chain(_todo,
                                                                    self.__working_urls, self.__done_urls)):
            self.__discarded_urls.add(url)
            discarded = 1
        else:
            self.__queue_todo.put_nowait((times, url))
            put = 1
        c = Counter(count=1, put=put, discarded=discarded)
        self.__counter.add(c)  # 更新计数器
        return c

    async def put_urls(self, urls: Iterable)->Counter:
        """
        向UlrsManager发送多条url
        参数:urls:为列表,元组或集合
        返回：urls的处理情况计数
        """
        c = self._counter(Counter)
        for url in urls:
            c.add(await self.put(url))
        return c

    async def task_done(self, url: str or URL, is_OK=True)->Counter:
        """
        通知UrlsManager本url的任务处理已经结束
        参数：is_OK 标识处理是否正常完成
        此方法在内部对url做完成或失败处理，并从working池中移除
        """
        self.__working_urls.remove(url)
        self.__total_key_urls[url] = self.__total_key_urls[url]+1
        if is_OK:
            self.__done_urls.add(url)
            c = Counter(done=1)
            self.__counter.add(c)
            self.__queue_todo.task_done()
            return c
        else:
            times=self.__total_key_urls[url]
            if times>=self.max_tried_times:
                self.__failed_urls.add(url)
                c = Counter(failed=1)
                self.__counter.add(c)
            else:
                c=await self.put(url)
            self.__queue_todo.task_done()
            return c

    async def get(self)->str:
        """从UrlsManager中获取1个url"""
        urlItem = await self.__queue_todo.get()  # 优先队列中存储的为tuple
        self.__working_urls.add(urlItem[1])
        return urlItem[1]

    async def get_urls(self, qty: int)->tuple:
        """取得多个url"""

    async def join(self)->bool:
        """阻塞执行线程直到UrlsManager中的url全部被取出并处理完 """
        await self.__queue_todo.join()
        return True

    async def get_todo(self):
        """得到待处理的url元组"""
        _todo = self.__queue_todo._queue
        _todo = (url for v, url in _todo)
        return tuple(_todo)
    async def get_results(self):
        #count put discarded done failed todo working
        _todo = self.__queue_todo._queue
        _todo = (url for v, url in _todo)
        _todo_urls=set(_todo)
        results=namedtuple('results','key_urls discarded_urls done_urls failed_urls todo_urls working_urls')
        results=results(self.__total_key_urls,self.__discarded_urls,self.__done_urls,self.__failed_urls,_todo_urls,self.__working_urls)
        return results
    async def get_count(self)->Counter:
        todo = len(self.__queue_todo._queue)
        working = len(self.__working_urls)
        ikeys = len(self.__total_key_urls)
        isum = sum(self.__total_key_urls.values())
        imin = min(self.__total_key_urls.values())
        imax = max(self.__total_key_urls.values())
        c = Counter(todo=todo, working=working, keys=ikeys,
                    sum=isum, min=imin, max=imax)
        self.__counter.update(c)
        return self.__counter.values
