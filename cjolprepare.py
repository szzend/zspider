import asyncio
import aiopg
import js2py
import re
from bs4 import BeautifulSoup as bs
from zspider import httpclient



async def _to_pg(sql:str,codes:list):
    """数据存储到pg"""
    dsn="host=192.168.43.254 user=pguser password=tofuture dbname=mydata"
    async with aiopg.create_pool(dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in codes:
                    await cur.execute(sql,i)

def get_class():
    """获取分类信息"""
    codes_url="http://js.cjolimg.com/v8/dict/cjol.dict.cn.min.js"
    codes=[]
    resp=httpclient.get(codes_url)
    result=js2py.eval_js(resp.text()).to_dict()['MPTR']
    for r in range(len(result)):
        d=result[r]
        codes.append((d['GID'],d['GN']))
        d_mp=d['MP']
        for r in range(len(d_mp)):
            d=d_mp[r]   #keys:C,I,K,N.C键值为[]
            codes.append((d['I'],d['N']))
            d_c=d['C']
            for r in range(len(d_c)):
                d=d_c[r]
                codes.append((d['I'],d['N']))
    return codes
def codes_to_pg(codes:list):
    sql="insert into cjol_codes values (%s,%s);"
    loop=asyncio.get_event_loop()
    loop.run_until_complete(_to_pg(sql,codes))

def get_zone():
    """获取地区编码"""
    zone_url="http://js.cjolimg.com/v7/drop_dictjs/location.js"
    results=[]
    resp=httpclient.get(zone_url)
    html=re.search(r'(<.+>)',resp.text()).group()
    tags=bs(html).select('input')
    attrs=[tag.attrs for tag in tags]
    for i in attrs:
        results.append((i['value'],i['txt']))
    return results
def zone_to_pg(zone:list):
    sql="insert into cjol_zone values (%s,%s);"
    loop=asyncio.get_event_loop()
    loop.run_until_complete(_to_pg(sql,zone))