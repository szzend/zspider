"""
python 3.7+
本模块包装aiohttp实现下载功能
Downloader类主要功能为根据设定的策略进行持续或批量下载,包括控制并发、延迟、自动设置代理等
"""
import asyncio
import aiohttp
import cchardet
from functools import partial
import typing
from .lib.selector import Selector
from .exception import  HttpClientException
import requests
from yarl import URL
from random import random

__all__=['HttpRequest','HttpResponse','BaseDownLoader']

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': 'http://s.cjol.com/',
    'Connection': 'keep-alive'
}

class HttpRequest:
    """
    封装请求参数，以此调用aiohttp.ClientSession.request
    callback为请求成功返回的回调函数:callback(HttpRequest,HttpReponse)
    except_callback 为请求异常的回调函数except_callback(HttpRequest,except)
    """
    __slots__ = ('method', 'url', 'session', 'callback',
                 'except_callback', 'kwargs')

    def __init__(self, url: str or URL,method: str='GET', session: bool = False, callback=None, except_callback=None, **kwargs):
        self.url,self.method,self.session, self.callback = url,method,session, callback
        self.kwargs = kwargs

    def __repr__(self):
        return f' <zspider.HTTPRequest[url:"{self.url},method:"{self.method}""]>'

    def __str__(self):
        return f' <HTTPRequest[url="{self.url}",method="{self.method}",session={self.session}, callback={self.callback},kwargs={self.kwargs}]>'

    def run(self):
        """//todo"""
    @property
    def agent(self):
        d={'url':self.url,'method':self.method,'session':self.session}
        d.update(self.kwargs)
        return dict(d)

class HttpResponse:
    """"""
    __slots__ = ('content', 'raw','agent')

    def __init__(self, content, resp,agent):
        self.content = content
        self.raw = resp
        self.agent=agent
    def __getattr__(self, name):
        try:
            return getattr(self.raw, name)
        except Exception as e:  # 异常处理需要改进
            raise e

    def __repr__(self):
        return f' <zspider.HttpRespone[{self.raw.method}:status:{self.raw.status}]>'

    def re(self, re: str):
        """实现正则选择器"""
        return Selector('re', self.content, re)

    def xpath(self, xpath: str):
        """实现xpath选择器"""
        return Selector('xpath', self.content, xpath)

    def css(self, css: str):
        """实现css选择器"""
        return Selector('css', self.content, css)
    def text(self, encoding: str = None):
        """将内容转换为文本"""
        if encoding:
            encoding = encoding
        else:
            encoding = cchardet.detect(self.content)['encoding']
        return self.content.decode(encoding, 'ignore')


class BaseDownLoader:
    """
    负责构建HttpRequest,然后调用相应的HttpAgent进行下载，控制下载过程
    """
    async def prepare(self):
        self._client_session = _get_client_session(timeout=self.timeout)
        self._client_no_session = _get_client_no_session(timeout=self.timeout)
        return True
    def __init__(self, **kwargs):
        """
        parameter：
        session：bool值，指示是否使用会话管理
        """
        self.timeout=aiohttp.ClientTimeout(total=10)


    def __make_request(self, url:str or URL,method:str=None):
        """
        根据爬取策略或资源调度封装参数至HttpRequest
        """
        m=method if method else 'GET'
        url=URL(url)
        return HttpRequest(url,m)

    async def fetch(self, url:str or URL,method:str=None,timeout:float=None)->HttpResponse:
        """
        获取指定url内容
        返回HttpResponse实例
        """
        timeout=aiohttp.ClientTimeout(total=float(timeout)) if timeout else self.timeout
        request = self.__make_request(url,method)
        callback = request.kwargs.pop('callback', None)
        except_callback = request.kwargs.pop('except_callback', None)
        agent=request.agent
        try:
            await asyncio.sleep(random()/8) #延迟
            resp=await self._client_no_session.request(url=request.url,method=request.method,timeout=timeout,headers=headers,**request.kwargs)
            content = await resp.read()
        except aiohttp.ClientError as e:
            if except_callback:
                except_callback(request, e)
            else:
                raise HttpClientException(e,agent)
        if callback:
            callback(request, HttpResponse(content, resp,agent))
        return HttpResponse(content, resp,agent)

    async def get(self,url):
        return self._client_no_session.request('POST',url)

    async def close(self):
        """
        关闭会话及连接
        """
        if not self._client_session.closed:
            await self._client_session.close()
        if not self._client_no_session.closed:
            await self._client_no_session.close()


def _get_client_session(**kwargs):
    _client = None
    _client = aiohttp.ClientSession(connector=_get_connector(),**kwargs)
    return _client


def _get_client_no_session(**kwargs):
    _client = None
    _client = aiohttp.ClientSession(
        connector=_get_connector(),cookie_jar=aiohttp.DummyCookieJar(),**kwargs)
    return _client


def _get_connector(**kwargs):
    return aiohttp.TCPConnector(ssl=False)

def post(url,**kwargs):
    return requests.post(url,**kwargs)
# 以下为requests同步实现，包装aiohttp.request API,使用同步式语法获取指定url内容
# todo:如何正常关闭底层的ClientSession ??
"""
async def request(method, url, **kwargs):
    content = None
    agent=dict(kwargs).update({'method':method,'url':url})
    async with aiohttp.request(method, url, **kwargs) as resp:
        content = await resp.read()
        return HttpResponse(content, resp,agent)


def _fetch(method, url, **kwargs):
    #调用aiohttp.request获取指定url内容
    #Parameters:
    #loop=asyncio.get_running_loop()   
    res = HttpRequest(url,method,**kwargs)
    resp = None
    callback = kwargs.pop('callback', None)
    except_callback = kwargs.pop('except_callback', None)
    try:
        resp = asyncio.run(request(method, url, **kwargs))
    except Exception as e:
        if except_callback:
            except_callback(e, res)
        else:
            raise e
    if callback:
        callback(resp, res)
    return resp


get = partial(_fetch, 'GET')
post = partial(_fetch, 'POST')
head = partial(_fetch, 'HEAD')
delete = partial(_fetch, 'DELETE')
put = partial(_fetch, 'PUT')
options = partial(_fetch, 'OPTIONS')
patch = partial(_fetch, 'PATCH')
"""