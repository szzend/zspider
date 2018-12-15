"""
"""
import asyncio
import types
from collections.abc import Iterable
from .httpclient import BaseDownLoader,HttpRequest
from .urlsmanager import UrlsManager
from .pipeline import PipeLine
from .exception import HttpClientException
import traceback
MAX_CONCURRENT_TASKS = 30  # 最大并发任务数

class Scheduler:
    """
    """

    def __init__(self, spider):
        """
        """
        self.spider = spider
        self.downloader = BaseDownLoader()
        self.urlsmanager = UrlsManager()
        self.tasks = None
    async def _prepare(self):
        """
        通知各组件准备
        """
        await self.urlsmanager.prepare()
        await self.downloader.prepare()
    async def _tap(self,item):
        """
        根据item进行分流
        //预设url以[]传送
        """
        if item and isinstance(item,list):
            if 'http' in item[0]:
                await self.urlsmanager.put_urls(item)
            else:
                await PipeLine.call(item,self.spider)
        else:
            await PipeLine.call(item,self.spider)
            

    async def worker(self):
        """
        将各个模块装配起来，形成工作流
        各个模块用协议接口形成交互
        """
        while True:
            try:
                url = await self.urlsmanager.get()
                response=await self.downloader.fetch(url)
                results=self.spider.parse(response)
                if isinstance(results,types.AsyncGeneratorType):
                    async for item in results:
                        await self._tap(item)                    
                else:
                    item=await results
                    await self._tap(item)   
                await self.urlsmanager.task_done(url)
            except HttpClientException as e:
                await self.urlsmanager.task_done(url,False)
            except Exception as e:
                await self.urlsmanager.task_done(url,False)
                print(e)
                traceback.print_exc()
                print(response)




    async def main(self):
        await self._prepare()
        await self.urlsmanager.put_urls(self.spider.prepare())
        self.tasks = [asyncio.create_task(self.worker())
                      for i in range(MAX_CONCURRENT_TASKS)]
        await self.urlsmanager.join()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.downloader.close()
        self.results=await self.urlsmanager.get_results()
        self.counter=await self.urlsmanager.get_count()


    def run(self):
        """ """
        loop=asyncio.get_event_loop()
        loop.run_until_complete(self.main())
        return self.counter
