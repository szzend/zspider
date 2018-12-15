import unittest
import asyncio
from .import cprint
from zspider import httpclient
from zspider.httpclient import HttpRequest,HttpResponse,BaseDownLoader

class TestHttpClient(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_httpclient(self):
        pass

    def test_HTTPRequest(self):
        """"""
        #req=HttpRequest('GET','http://www.163.com',timeout=3)
        #cprint(str(req))
        #req=HttpRequest('GET','abc',verify_ssl=1)
        #cprint(str(req))

    def test_Downloader(self):
        url='http://www.126.com'
        d=BaseDownLoader()
        loop=asyncio.get_event_loop()
        r=loop.run_until_complete(d.fetch(url))
        cprint(str(r.text()))


import aiohttp
session=aiohttp.ClientSession()
async def a():
    resp=await session.request('POST','http://www.baidu.com')
    print(resp)

async def b():
    await a()

async def c():
    await b()