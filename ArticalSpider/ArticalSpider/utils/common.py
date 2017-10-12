import hashlib
import re

def get_md5(url):
    if isinstance(url, str):#str represents uni-code
        url = url.encode("utf-8")
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()

def extract_num(value):
    #从字符串中提取出数字
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0
    return nums