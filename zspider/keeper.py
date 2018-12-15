"""
运行环境
python 3.7+
"""
from collections import namedtuple, OrderedDict

#以下为要包装的对象：1个命名元组，用于存储计数，并对外传递信息
Counter = namedtuple("Counter", "total put OK failed recorded keys count",
                     defaults=(0, 0, 0, 0, 0, 0, 0))

class Demo:
    """
    使用__slots__,__new__及描述符实现定制类
    只能使用Counter中的属性进行存取
    """
    #引入__slots__特殊属性用于控制实例属性
    #_dict用于存储内部数据，此处未实现保护,可用定制描述符实现保护
    __slots__ = list(Counter._fields)+['_dict']
    def __new__(cls):
        """
        使用__new__特殊方法用于生成类实例之前配置类模板
        因为描述符及特性(property)都是附在类上，而不是在实例上
        """
        for f in Counter._fields:
            #动态生成描述符或特性，用于控制属性存取
            #python内置的property就是特殊的描述符，可以用partialmethod包装后在此处使用.此处未展示
            #自己实现描述符当然更具定制性
            setattr(cls,f,Descriptor(f))
        return super().__new__(cls)
    def __init__(self):
        self._dict={}
        for f in Counter._fields:
            self._dict[f]=0

    def update(self, n: Counter = None, **kargs):
        """
        使用数值累加计数器
        当Counter与键参数同时提供时，键值为准
        """
        
#描述符实现
class Descriptor:
    def __init__(self,storage_name:str):
        self.storage_name=storage_name
    def __set__(self,instance,value):
        print('from descriptor..')
        instance._dict[self.storage_name]=value
    def __get__(self,instance,owner):
        return instance._dict[self.storage_name]
    
#也可以用以下特性工厂函数实现
def make_property(store_name:str)->property:
    def getter(instance):
        print('from property getter')
        return instance._dict[store_name]
    def setter(instance,value):
        print('from property setter')
        instance._dict[store_name]=value
    return property(getter,setter)


