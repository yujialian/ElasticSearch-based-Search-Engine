import scrapy
import time
import re
from PIL import Image
import matplotlib.pyplot as plt


def start_requests(self):
    t = str(int(time.time() * 1000))
    captcha_url = "https://www.zhihu.com/captcha.gif?r=%s&type=login&lang=cn" % (t)
    return [scrapy.Request(captcha_url, headers=self.headers, callback=self.get_captcha)]


def get_captcha(self, response):
    if response.status == 200:
        with open('captcha.jpg', 'wb') as f:
            f.write(response.body)
        try:
            with Image.open('captcha.jpg') as scan:
                scan.show()
        except:
            pass
        try:
            with Image.open('captcha.jpg') as im:
                im = im.resize((200, 44))
                pick = int(input("请输入倒立文字数量：\n>"))
                plt.imshow(im)
                pos = plt.ginput(pick)
                plt.show()
        except Exception as e:
            print(e)
        return [scrapy.Request(url="http://www.zhihu.com/", headers=self.headers, meta={'captcha': pos},
                               callback=self.login)]


def login(self, response):
    response_text = response.text
    match_obj = re.match('.*name="_xsrf" value="(.*?)"', response_text, re.DOTALL)
    xsrf = ''


    captcha_data = response.meta['captcha']
    captcha_data = str(captcha_data).replace('(', '[')
    captcha_data = captcha_data.replace(')', ']')
    captcha = '{"img_size":[200,44],"input_points":%s}' % captcha_data
    if match_obj:
        xsrf = (match_obj.group(1))
    if xsrf:
        post_url = "https://www.zhihu.com/login/phone_num"
    post_data = {
        "_xsrf": xsrf,
        "phone_num": "1501141032",
        "password": "*********",
        'captcha_type': 'cn',
        'captcha': captcha
    }

    return [scrapy.FormRequest(
        url=post_url,
        formdata=post_data,
        headers=self.headers,
        callback=self.check_login
    )]