import asyncio
import unittest
from zspider.pipeline import PipeLine,ItemPipe
from . import cprint
from zspider.spiders import BaseSpider

class Test_pipeline(unittest.TestCase):

    def setUp(self):
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop=asyncio.get_event_loop()
    def tearDown(self):
        self.loop.close()

    def test_pipeline_call(self):
        @PipeLine.register(BaseSpider)
        class a(ItemPipe):
            def process(self,item,spider):
                pass
        
        @PipeLine.register(BaseSpider,1)
        class b(ItemPipe):
            def process(self,item,spider):
                pass
        
        @PipeLine.register(BaseSpider)
        class c(ItemPipe):
            def process(self,item,spider):
                pass

        @PipeLine.register_global()
        class d(ItemPipe):
            def process(self,item,spider):
                pass
        @PipeLine.register_global(3,is_backend=False)
        class e(ItemPipe):
            def process(self,item,spider):
                pass
        b=BaseSpider()
        result=self.loop.run_until_complete(PipeLine.call('ac',b))
        cprint(str(result))