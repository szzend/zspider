"""
"""
import asyncio
import random
import unittest
from .import cprint
from zspider.urlsmanager import Counter, UrlsManager


class TestUrlsManager(unittest.TestCase):
    """

    """

    def asyncio_helper(self, fun):
        pass

    def test_init(self):
        """测试UrlsManager是否正常初始化"""
        u = UrlsManager()
        assert isinstance(u, UrlsManager)
        u = UrlsManager(5)
        assert isinstance(u, UrlsManager)

    def test_prepare(self):
        """测试UrlsManager.prepare"""
        u = UrlsManager()
        r = self.loop.run_until_complete(u.prepare())
        assert r

    def test_put(self):
        """测试UrlsManager.put及.get_count方法"""
        self.loop.run_until_complete(self.man1.prepare())
        n = self.loop.run_until_complete(self.man1.put(self.raw_urls[0]))
        self.assertEqual(n.put, 1)
        n = self.loop.run_until_complete(self.man1.put(self.raw_urls[0]))
        self.assertEqual(n.discarded, 1)
        t = self.loop.run_until_complete(self.man1.get_todo())
        r = self.loop.run_until_complete(self.man1.get_count())
        assert r.count == 2 and r.put == r.todo == r.keys == 1 and r.sum == 0
        cprint(str(t))
    def test_put_urls(self):
        """测试UrlsManager.put_urls及.get_count方法"""
        self.loop.run_until_complete(self.man1.prepare())
        n = self.loop.run_until_complete(self.man1.put_urls(self.raw_urls[:3]))
        assert n.count == n.put == 3
        r = self.loop.run_until_complete(self.man1.get_count())
        assert r.count == r.put == r.todo == r.keys == 3 and r.sum == 0

    def test_get(self):
        """测试UrlsManager.get及.get_count方法"""
        self.loop.run_until_complete(self.man1.prepare())
        self.loop.run_until_complete(self.man1.put(self.raw_urls[0]))
        url = self.loop.run_until_complete(self.man1.get())
        self.assertIsInstance(url, str)
        r = self.loop.run_until_complete(self.man1.get_count())
        assert r.working == 1

    def test_task_done(self):
        """测试UrlsManager.task_done及.get_count及方法"""
        self.loop.run_until_complete(self.man1.prepare())
        self.loop.run_until_complete(self.man1.put_urls(self.raw_urls[:3]))
        url1 = self.loop.run_until_complete(self.man1.get())
        url2 = self.loop.run_until_complete(self.man1.get())
        n1 = self.loop.run_until_complete(
            self.man1.task_done(url2, is_OK=True, agent={}))
        n2 = self.loop.run_until_complete(
            self.man1.task_done(url1, is_OK=False, agent={}))
        assert n1.done == n2.failed == 1
        r = self.loop.run_until_complete(self.man1.get_count())
        assert r.count == r.put == 4
        assert r.keys == 3
        assert r.working == 0

    def setUp(self):
        self.man1 = UrlsManager(3)
        self.man2 = UrlsManager(5)
        # 生成测试数据
        self.raw_urls = ["".join(random.sample(
            '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIXYZ', 9)) for i in range(100)]
        self.set_urls = set(self.raw_urls)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()


if __name__ == "__main__":
    suit = unittest.TestSuite()
    suit.addTest(TestUrlsManager.test_init)
    suit.addTest(TestUrlsManager.test_prepare)

    runner = unittest.TextTestRunner()
    runner.run(suit)
