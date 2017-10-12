# -*- coding: utf-8 -*-
import scrapy
import re
import json
import datetime
from urllib import parse
from scrapy.loader import ItemLoader
from items import ZhihuQuestionItem, ZhihuAnswerItem

class ZhihuSpider(scrapy.Spider):

    name = "zhihu"
    allowed_domains = ["www.zhihu.com"]
    start_urls = ['http://www.zhihu.com/']
    #question的answer第一页的请求url
    start_answer_url = "http://www.zhihu.com/api/v4/questions/{0}/answers?sort_by=default&include=data%5B%2A%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B%2A%5D.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics&limit={1}&offset={2}"
    agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
    headers = {
    "HOST":"www.zhihu.com",
    "Referer": "www.zhihu.com",
    'User-Agent': agent
    }

    custom_settings = {
        "COOKIES_ENABLED": True
    }

    """
    parse() 是spider的一个方法。 被调用时，每个初始URL完成下载后生成的 Response 对象将会作为唯一的参数传递给该函数。 
    该方法负责解析返回的数据(response data)，提取数据(生成item)以及生成需要进一步处理的URL的 Request 对象。
    """

    def parse(self, response):
        #提取出html页面中的所有url  并且跟踪这些url进行进一步爬取
        #如果提取的URL中格式为／question／**********：就可以下载之后进入解析函数
        all_urls = response.css("a::attr(href)").extract()
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]
        all_urls = filter(lambda x: True if x.startswith("https") else False, all_urls)
        for url in all_urls:
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", url)#/或者$结尾符
            if match_obj:
                request_url = match_obj.group(1)
                #如果提取到question相关的页面则下载后交由提取函数进行提取处理
                yield scrapy.Request(request_url, headers=self.headers, callback=self.parse_question)
            else:
                #如果不是question页面则直接进一步跟踪
                if "/logout" not in url:
                    yield scrapy.Request(url, headers=self.headers, callback=self.parse)

    def parse_question(self, response):
        #处理question页面，从页面中提取出具体的question item
        if "QuestionHeader-title" in response.text:
            #处理知乎新版本
            match_obj = re.match("(.*zhihu.com/question/(\d+))(/|$).*", response.url)#/或者$结尾符
            if match_obj:
                question_id = int(match_obj.group(2))
            item_loader = ItemLoader(item=ZhihuQuestionItem(), response=response)
            item_loader.add_css("title", "h1.QuestionHeader-title::text")
            item_loader.add_css("content", ".QuestionHeader-detail span::text")
            item_loader.add_value("url", response.url)
            item_loader.add_value("zhihu_id", question_id)
            item_loader.add_css("answer_num", ".List-headerText span::text")
            item_loader.add_css("comments_num", ".QuestionHeaderActions button::text")
            item_loader.add_css("watch_user_num", ".NumberBoard-value::text")
            item_loader.add_css("topics", ".QuestionHeader-topics .Popover div::text")
            question_item = item_loader.load_item()
            print(self.start_answer_url.format(question_id, 20, 0))
            #yield scrapy.Request(self.start_answer_url.format(question_id, 20, 0), headers=self.headers, callback=self.parse_answer,dont_filter=True)
            #解析完成之后首先将下一个即将爬取的页面
            yield question_item


    def parse_answer(self, response):
        #处理question的answer
        ans_json =json.loads(response.text)
        is_end = ans_json["paging"]["is_end"]
        next_url = ans_json["paging"]["next"]

        #提取answer的具体字段
        for answer in ans_json["data"]:
            answer_item = ZhihuAnswerItem()
            answer_item["zhihu_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] =answer["author"]["id"] if "id" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer["content"] else answer["excerpt"]
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["answer_excerpt"] = answer["excerpt"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.datetime.now()

            yield answer_item


        if not is_end:
            yield scrapy.Request(next_url, headers=self.headers, callback=self.parse_answer)


    def start_requests(self):
        return [scrapy.Request('https://www.zhihu.com/#signin', headers=self.headers, callback=self.login)]



    def login(self, response):
        response_text = response.text
        match_obj = re.match('.*name="_xsrf" value="(.*?)"', response.text, re.DOTALL)
        xsrf = ''
        if match_obj:
            xsrf = (match_obj.group(1))

        if xsrf:
            post_url = "https://www.zhihu.com/login/phone_num",
            post_data = {
                "_xsrf": xsrf,
                "phone_num": "3042169199",
                "password": "lyj8623656",
                "captcha": ""
            }

            import time
            t = str(int(time.time() * 1000))
            captcha_url = "https://www.zhihu.com/captcha.gif?r={0}&type=login".format(t)
            yield scrapy.Request(captcha_url, headers=self.headers, meta={"post_data":post_data}, callback= self.login_after_captcha)#用到知乎cookie架的cookie，这样保证在同一个cookie下




    def login_after_captcha(self, response):

        with open("captcha.jpg", "wb") as f:
            f.write(response.body)
            if f:
                f.close()

        from PIL import Image
        try:
            im = Image.open('captcha.jpg')
            im.show()
            if im:
                im.close()
        except:
            print("WRONG LOAD!")
        captcha = input("please input the varify code:\n>")

        post_data = response.meta.get("post_data", {})
        post_url = "https://www.zhihu.com/login/phone_num"
        post_data["captcha"] = captcha
        return [scrapy.FormRequest(
            url=post_url,
            formdata=post_data,
            headers=self.headers,
            callback=self.check_login  # check if login(if ignore the call back function, will call parse by default
        )]

    def check_login(self, response):
        #check if login successful
        text_json = json.loads(response.text)
        if "msg" in text_json and text_json["msg"] == "登录成功":
            for url in self.start_urls:
                yield scrapy.Request(url, dont_filter=True, headers=self.headers)