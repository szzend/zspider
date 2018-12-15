"""
"""
__all__=['HttpClientException']

class HttpClientException(Exception):
    def __init__(self,e,agent):
        self.raw_exception=e
        self.agent=agent
    
    def __repr__(self):
        return f' <HttpClientException[{self.raw_exception.__class__.name},agent]>'
    def __str__(self):
        return str(self.agent)
