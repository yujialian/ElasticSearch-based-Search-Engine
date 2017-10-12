# -*- coding: utf-8 -*-
# Define here the models for your scraped items
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import datetime
import scrapy
import re
import redis
from scrapy.loader import ItemLoader
from settings import SQL_DATETIME_FORMAT
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from models.es_types import ArticleType, LagouType, ZhihuAnswerType, ZhihuQuestionType
from elasticsearch_dsl.connections import connections


es=connections.create_connection(ArticleType._doc_type.using)

redis_cli = redis.StrictRedis()

from w3lib.html import remove_tags

from utils.common import extract_num



def add_jobbole(value):
    return value+"-bobby"

def date_convert(value):
    try:
        create_date = datetime.datetime.strptime(value, "%Y/%m/%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()

    return create_date


def get_nums(value):
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums

def remove_comment_tags(value):
    #去掉tag中提取的评论
    if "评论" in value:
        return ""
    else:
        return value

def return_value(value):
    return value

class ArticleItemLoader(ItemLoader):
    #自定义itemloader
    default_output_processor=TakeFirst()

def get_suggests(index, info_tuple):
    #generate search suggestion based on string
    used_words = set()#prevant duplicates suggestion
    suggests = []
    for text, weight in info_tuple:
        if text:
            #call elasticsearch's analyzer, analyze string
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':["lowercase"]}, body=text)
            analyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = analyzed_words - used_words
            used_words.update(words)
        else:
            new_words = set()

        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})

    return suggests

class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert)
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)#覆盖掉defalut output processor
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into jobbole_article(title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(content),
            fav_nums=VALUES(fav_nums)
        """
        params = (self["title"], self["url"], self["create_date"], self["fav_nums"])

        return insert_sql, params



    def save_to_es(self):
        #turn the item in the ES's item
        article = ArticleType()
        article.title = self["title"]
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = get_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7), (article.content, 4)))
        article.save()
        redis_cli.incr("jobbole_count")
        return

class ZhihuQuestionItem(scrapy.Item):
    #zhihu的问题Item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        #插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_question(zhihu_id, topics, url, title, content, answer_num, comments_num,
            watch_user_num, click_num, crawl_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
             ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num),
             watch_user_num=VALUES(watch_user_num),click_num=VALUES(click_num),crawl_time=VALUES(crawl_time)
        """
        zhihu_id = self["zhihu_id"][0]
        topics = ",".join(self["topics"])#传入的是只有一个元素的list，所以可以用join成一个值
        url = self["url"][0]#传进来的list是一个值，直接去第0个也是可行的
        title = "".join(self["title"])
        if self["content"]:
            content = "".join(self["content"])
        else:
            content = ""
        answer_num = extract_num("".join(self["answer_num"]))
        comments_num = extract_num("".join(self["comments_num"]))
        if len(self["watch_user_num"]) == 2:
            watch_user_num = int(self["watch_user_num"][0])
            click_num = int(self["watch_user_num"][1])
        else:
            watch_user_num = int(self["watch_user_num"][0])
            click_num = 0
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (zhihu_id, topics, url, title, content, answer_num, comments_num,
                  watch_user_num, click_num, crawl_time)
        return insert_sql, params

    def save_to_es(self):
        #turn the item in the ES's item
        question = ZhihuQuestionType()
        question.zhihu_id = self["zhihu_id"][0]
        question.topics = ",".join(self["topics"])
        question.url = self["url"][0]
        question.title = "".join(self["title"])
        if "content" in self:
            if self["content"]:
                question.content = "".join(self["content"])
        else:
            question.content = "No content"
        question.answer_num = extract_num("".join(self["answer_num"]))
        question.comments_num = extract_num("".join(self["comments_num"]))
        if len(self["watch_user_num"]) == 2:
            question.watch_user_num = int(self["watch_user_num"][0])
            question.click_num = int(self["watch_user_num"][1])
        else:
            question.watch_user_num = int(self["watch_user_num"][0])
            question.click_num = 0
        question.crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        question.suggest = get_suggests(ZhihuQuestionType._doc_type.index, ((question.title, 10), (question.topics, 7), (question.content, 5)))

        question.save()
        redis_cli.incr("question_count")
        return

class ZhihuAnswerItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    answer_excerpt = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        #插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, 
            comments_num, create_time, update_time, crawl_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
             ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num = VALUES(comments_num),
            praise_num = VALUES(praise_num), update_time = VALUES(update_time)
        """
        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = (
            self["zhihu_id"], self["url"], self["question_id"], self["author_id"], self["content"],
            self["praise_num"], self["comments_num"], create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )
        return insert_sql, params

    def save_to_es(self):
        #turn the item in the ES's item
        answer = ZhihuAnswerType()
        answer.zhihu_id = self["zhihu_id"]
        answer.url = self["url"]
        answer.question_id = self["question_id"]
        answer.author_id = self["author_id"]
        answer.answer_excerpt = self["answer_excerpt"]
        answer.content = self["content"]
        answer.praise_num = self["praise_num"]
        answer.comments_num = self["comments_num"]
        answer.create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        answer.update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        answer.crawl_time = self["crawl_time"]
        answer.suggest = get_suggests(ZhihuAnswerType._doc_type.index, ((answer.content, 5),))

        answer.save()
        redis_cli.incr("ans_count")
        return

class LagouJobItemLoader(ItemLoader):
    #自定义itemloader
    default_output_processor=TakeFirst()


def remove_slash(value):
    #去掉工作城市的'／'
    return value.replace("/", "")

def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list= [item.strip() for item in addr_list if item.strip()!="查看地图"]
    return "".join(addr_list)

def handle_strip(value):
    return value.strip()

def handle_desc(value):
    value=value.replace("<div>", "")
    value=value.replace("<p>", "")
    value=value.replace("</p>", "")
    value=value.replace("<br>", "")
    return value.replace("\n", "")

def get_min_wages(value):
    import re
    num_str=re.match("^\d+", value)
    return int(num_str.group(0)) * 1000


class LagouJobItem(scrapy.Item):
    #拉勾网职位信息
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field(
        input_processor=MapCompose(get_min_wages),
    )
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_slash),#调用函数名称即可
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_slash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_slash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field(
        input_processor=MapCompose(handle_desc),
    )
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr),
    )
    company_name = scrapy.Field(
        input_processor=MapCompose(handle_strip),
        #input_processor=MapCompose(handle_desc),
    )
    company_url = scrapy.Field()
    tags = scrapy.Field(
        output_processor=Join(",")
    )
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url_object_id, url, salary, job_city, work_years, degree_need, job_type,
             publish_time, job_advantage, job_desc, job_addr, company_name, company_url, 
            tags, crawl_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc),
            job_advantage=VALUES(job_advantage), tags = VALUES(tags)
            
        """
        params = (
            self["title"], self["url"], self["url_object_id"], self["salary"], self["job_city"],
            self["work_years"], self["degree_need"], self["job_type"], self["publish_time"], self["job_advantage"],
            self["job_desc"], self["job_addr"], self["company_name"], self["company_url"], self["tags"],
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )
        return insert_sql, params

    def save_to_es(self):
        #turn the item in the ES's item
        job = LagouType()
        job.title = self["title"]
        job.url = self["url"]
        job.url_object_id = self["url_object_id"]
        job.salary = self["salary"]
        job.job_city = self["job_city"]
        job.degree_need = self["degree_need"]
        job.job_type = self["job_type"]
        job.job_advantage = self["job_advantage"]
        job.job_desc = self["job_desc"]
        job.job_addr = self["job_addr"]
        job.company_name = self["company_name"]
        job.company_url = self["company_url"]
        job.tags = self["tags"]
        job.suggest = get_suggests(LagouType._doc_type.index, ((job.title, 10), (job.tags, 7), (job.job_desc, 4), (job.job_type, 3)))
        job.save()
        redis_cli.incr("job_count")
        return
