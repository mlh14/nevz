

def running_time(func):
    import datetime
    import functools
    @functools.wraps(func)
    def wrapper(*args, **kw):
        start_time = datetime.datetime.now()
        ret = func(*args, **kw)
        end_time = datetime.datetime.now()
        print '[%s()] done, run time : %r sec' % (func.__name__, (end_time - start_time).seconds)
        return ret
    return wrapper
    
    
def dict_split(d, size=None, cnt=1):
    """
    Split dict to small dicts
    -------------
    Parameter
    cnt :   块数
    size:   块大小
    """
    import math
    dict_size = len(d) if len(d) > 0 else -1
    cnt, size = int(cnt), int(size) if size else None
    if not size:
        cnt = cnt if cnt > 0 else 1
        cnt = min(cnt, dict_size)
        size = int(math.ceil(float(dict_size) / cnt))
    else:
        size = size if size > 0 else dict_size
        size = min(size, dict_size)
        cnt  = int(math.ceil(float(dict_size) / size))
    items = d.items()
    for i in xrange(cnt):
        i = i * size
        yield dict(items[i:i + size])
