"""
包装解析器功能
"""
import asyncio

__all__=['BaseSpider']
class BaseSpider:
    """spider基类，实现基本接口"""

    def prepare(self):
        """
        配置spider，准备初始任务
        返回：可迭代的urls
        """
    async def parse(self, response):
        """
        解析获得所需要的内容
        标准实现模式为一个异步生成器
        yield 1个item或 HttpRequest或[item or HttpRequest],分别由调度器送到pipeline处理。
        如果实现为协程则调度器丢弃此协程的返回结果
        """
    