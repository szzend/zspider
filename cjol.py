from zspider import PipeLine,BaseSpider,Scheduler,HttpRequest,httpclient,ItemPipe
import asyncio
from yarl import URL
import re
import json
from bs4 import BeautifulSoup as bs
from lxml import etree
from collections import defaultdict
from datetime import datetime
import aiopg
import math
from psycopg2 import ProgrammingError

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': 'http://s.cjol.com/',
    'Connection': 'keep-alive'
}


class CJOL(BaseSpider):
    def __init__(self):
        self.d={}
    def prepare(self):

        self.__companys=set()
        # 查询样式 http://s.cjol.com/service/joblistjson.aspx?KeyWord=工程师&Function=050105,010202,050202&Location=32,2008&SearchType=3&ListType=2&page=2
        url = "http://s.cjol.com/service/joblistjson.aspx"
        # 查询深圳区域7天内更新过的招聘信息
        params = {
            #"KeywordType": "",  # 1为职位，2为公司，3为公司+职位，省略则为ALL
            "KeyWord": "ERP",  # 查询关键词
            #"Function": "",
            "Location": 2008,
            "PostingPeriod": 7,  # 更新期间1,3,7,14,30
            "Minsalary":8000,
            "Maxsalary":16000,
            "defaultmust":0,
            "SearchType": 3,
            "ListType": 2,
            "page": 1
        }
        u = URL(url)
        new_url = u.with_query(**params)
        resp = httpclient.post(new_url)
        d = json.loads(resp.text)
        count = d['RecordSum']  # 总记录条数
        print(f'-----------total:{count}')
        NUM=40  #每页40条
        pages = math.ceil(count/NUM)
        urls = []
        for i in range(1, pages+1):
            params['page'] = i
            urls.append(u.with_query(params))
        #urls.reverse()
        #half=int(len(urls)/2)
        #print(half)
        return urls

    async def parse(self, response):
        url=str(response.real_url)
        g=re.search(r'(page=\d+)',url)
        if g:
            self.d[g.group(1)]=(response.status,response)
        head = ['职位名称', '公司名称', '工作地区', '学历', '经验', '月薪', '更新时间']
        # 以下处理json职位搜索结果页面
        if 'json' in response.content_type:
            d = json.loads(response.content)
            html=etree.HTML(d['JobListHtml'])
            _head=html.xpath('//ul[@id="resultlist_head"]/li/text()')#标题
            if head==_head:
                records=html.xpath('//ul[@class="results_list_box"]/li/text()')
                job_id=html.xpath('//ul[@class="results_list_box"]/li/input/@value')
                company_id=html.xpath('//ul[@class="results_list_box"]/li/input/@companyid')
                job_name=html.xpath('//ul[@class="results_list_box"]/li/h3/a')
                job_name=[e.xpath('string(.)') for e in job_name]
                assert len(job_id)==len(job_name)==len(company_id)
                job_links=html.xpath('//ul[@class="results_list_box"]/li/h3/a/@href')   #job description page
                yield job_links
                company_links=html.xpath('//li[@class="list_type_second"]/a/@href') #company brief page
                yield company_links
                tails=list(zip(job_id,job_name,company_id))
                step=5
                num=len(records)
                assert num%step==0
                items=[]
                for i in range(int(num/step)):
                    record=tuple(records[step*i:step*(i+1)+1])
                    record=tails[i]+record
                    items.append(record)
                yield items
        # 获取职位资料或公司资料
        else:
            url = str(response.url)
            job_id = re.search(r'job-(\d+)', url)
            company_id = re.search(r'company-(\d+)', url)
            if job_id:
                job_id = job_id.group(1)
                html = etree.HTML(response.content)
                job_desc =','.join(html.xpath('//div[@class="coninfo-jobdesc"]/p/text()'))
                job_class=html.xpath('//dt[contains(text(),"岗位分类")]/following-sibling::dd[1]/a/@href')
                if job_class:
                    job_class=[re.search(r'f(\d+)',i).group(1) for i in job_class]
                yield (job_id,job_class,job_desc)
            elif company_id:
                company_id = company_id.group(1)
                if company_id in self.__companys:  # 忽略已经获取过资料的公司
                    pass
                else:
                    html = etree.HTML(response.content)
                    company_name = html.xpath('//div[@class="name-combscinfo"]/text()')
                    others=html.xpath('//div[contains(@class,"dl-combascinfo")]/dl/dd/text()')
                    # (company_id,company_name,industry,size,nature,website)
                    website=html.xpath('//dt[contains(text(),"网址")]/following-sibling::dd[1]/a/@href')
                    address = html.xpath('//dt[contains(text(),"地址")]/following-sibling::dd[1]/text()')
                    brief=','.join(html.xpath('//div[@class="coninfo-combscintro"]/*/text()'))
                    website=website if website else [""]
                    brief=brief if brief else [""]
                    yield (company_id,company_name[0])+tuple(others[:3])+(website[0],address[0],brief[0])


@PipeLine.register(CJOL)
class DataClean(ItemPipe):
    """
    去除空格,转换字段格式等
    """
    def __init__(self):
        self._pool=defaultdict(dict)
    async def process(self,item,spider):
        if not item:
            return
        length=len(item)
        if length==8 and isinstance(item,tuple):
            return self._do_company(item)
        elif length==3:
            return self._do_job_tail(item)
        else:
            return self._do_job(item)

    def _do_job(self,item):
        #head = ['id','职位名称', '公司名称', '工作地区', '学历', '经验', '月薪', '更新时间']
        pg_fields=('code','name','company_id','address','qualifications','pay','job_date','update_date')
        for record in item:
            pay=record[6]
            g=re.search(r'(\d+\.\d+|\d+)-(\d+\.\d+|\d+)',pay)
            if g:
                low=int(float(g.group(1))*1000)
                high=int(float(g.group(2))*1000)
            else:
                low=high=0
            pay=f'[{low},{high}]'
            today=datetime.today()
            job_date=f'{today.year}-{record[7]}'
            job_date=datetime.strptime(job_date,'%Y-%m-%d')
            if job_date>today:
                job_date=f'{today.year-1}-{record[7]}'
            record=record[:5]+(pay,job_date.strftime('%Y-%m-%d'),today.strftime('%Y-%m-%d'))
            d=dict(zip(pg_fields,record))
            result=self._pool[record[0]]
            result.update(d)
            if len(result)>8:
                return self._pool.pop(record[0])
       
    def _do_job_tail(self,item):
        pg_fields=('job_class','brief')
        brief=item[2]
        brief=re.sub(r'\s',"",brief)
        job_class=','.join(item[1])
        job_class="{%s}" % job_class
        d=dict(zip(pg_fields,(job_class,brief)))
        result=self._pool[item[0]]
        result.update(d)
        if len(result)>8:
            return self._pool.pop(item[0])
    def _do_company(self,item):
        pg_fields=('code','name','industry','size','nature','website','address','brief')
        record=[re.sub(r'\s',"",i) for i in item]
        if len(record[5])>60:
            record[5]=record[:40]
        d=dict(zip(pg_fields,record))
        return d

@PipeLine.register(CJOL)
class ToPG(ItemPipe):
    def __init__(self):
        self.dsn="host=192.168.43.254 user=pguser password=tofuture dbname=mydata"
        self.tb_job='insert into cjol_jobs %s values %s;'
        self.tb_company='insert into cjol_companys %s values %s;'
        self.counter=0
    async def process(self,item,spider):
        self.counter=self.counter+1
        if not item:
            return
        cols=f'({",".join(item.keys())})'
        s=['%s' for i in range(len(item))]
        s=f'({",".join(s)})'
        values=tuple(item.values())
        length=len(item)
        if length==10:
            sql=self.tb_job % (cols,s)
        elif length==8:
            sql=self.tb_company % (cols,s)
        try:
            await self._topg(sql,values)
        except ProgrammingError as e:
            print(sql)
            raise e

    async def _topg(self,sql,values):
        async with aiopg.create_pool(self.dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql,values)