import requests
try:
    import cookielib
except:
    import http.cookiejar as cookielib

import re


#用session请求才能成功，一次session一次会话
session = requests.session()
session.cookies = cookielib.LWPCookieJar(filename="cookies.txt")
try:
    session.cookies.load(ignore_discard=True)
except:
    print("cookies can not been load")
agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
header = {
    "HOST":"www.zhihu.com",
    "Referer": "www.zhihu.com",
    'User-agent': agent
}

def is_login():
    #though try personal center page to assesset whether or not it's login status
    inbox_url = "https://www.zhihu.com/inbox"
    response=session.get(inbox_url, headers=header, allow_redirects=False)
    if response.status_code != 200:
        return False
    else:
        return True

def get_xsrf():
    #Get xsrf code
    response=session.get("https://www.zhihu.com", headers=header)
    match_obj = re.match('.*name="_xsrf" value="(.*?)"', response.text)
    if match_obj:
        return (match_obj[1])
    else:
        return ""

def get_index():
    response = session.get("https://www.zhihu.com", headers=header)
    with open("index_page.html", "wb") as f:
        f.write(response.text.encode("utf-8"))
    print("ok")

def get_captcha():
    import time
    t = str(int(time.time()*1000))
    captcha_url = "https://www.zhihu.com/captcha.gif?r={0}&type=login".format(t)
    t=session.get(captcha_url, headers = header)
    with open("captcha.jpg", "wb") as f:
        f.write(t.content)
        f.close()

    from PIL import Image
    try:
        im = Image.open('captcha.jpg')
        im.show()
        im.close()
    except:
        pass
    captcha = input("please input the varify code:\n>")
    return captcha

def zhihu_login(account, password):
    #zhihu login information
    if re.match("^3\d{9}", account):
        print("Login with phone number")
        post_url = "https://www.zhihu.com/login/phone_num"
        #captcha = get_captcha()
        post_data = {
            "_xsrf": get_xsrf(),
            "phone_num": account,
            "password": password,
            "captcha": get_captcha() #
        }
    else:
        if "@" in account:
            #login with email address
            print("邮箱方式登录")
            post_url = "https://www.zhihu.com/login/email"
            post_data = {
                "_xsrf": get_xsrf(),
                "email": account,
                "password": password,
                "captcha": ""
            }
    response_text = session.post(post_url, data=post_data, headers=header)
    session.cookies.save()
        #save the cookie to response

zhihu_login("3042169199", "lyj8623656")
is_login()

