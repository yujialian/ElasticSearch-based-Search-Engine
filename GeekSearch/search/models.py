from django.db import models

# Create your models here.
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["localhost"])

class CustomAnalyzer(_CustomAnalyzer):#search suggestion
    def get_analysis_definition(self):
        return {}

ik_analyzer = CustomAnalyzer('ik_max_word',filter=['lowercase'])

class ArticleType(DocType):
    #jobbole article type
    suggest = Completion(analyzer=ik_analyzer)
    title = Text(analyzer="ik_max_word")
    create_date = Date()
    url = Keyword()
    url_object_id = Keyword()
    front_image_url = Keyword()
    front_image_path = Keyword()
    praise_nums = Integer()
    comment_nums = Integer()
    fav_nums = Integer()
    tags = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")

    class Meta:
        index = "geek_search"
        doc_type = "article"

class LagouType(DocType):
    #lagou job website
    suggest = Completion(analyzer=ik_analyzer)
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    salary = Text(analyzer="ik_max_word")
    job_city = Keyword()
    degree_need = Keyword()
    job_type = Keyword()
    job_advantage = Keyword()
    job_desc = Text(analyzer="ik_max_word")
    job_addr = Text(analyzer="ik_max_word")
    company_name = Text(analyzer="ik_max_word")
    company_url = Keyword()
    tags = Text(analyzer="ik_max_word")

    class Meta:
        index = "geek_search"
        doc_type = "job"



class ZhihuAnswerType(DocType):
    #zhihu answer type
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    url = Keyword()
    question_id = Keyword()
    author_id = Text()
    #new
    answer_excerpt = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")
    praise_num = Integer()
    comments_num = Integer()
    create_time = Date()
    update_time = Date()
    crawl_time = Date()

    class Meta:
        index = "geek_search"
        doc_type = "zhihu_ans"

class ZhihuQuestionType(DocType):
    #zhihu question type
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    topics = Text(analyzer="ik_max_word")
    url = Keyword()
    title = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")
    answer_num = Integer()
    comments_num = Integer()
    watch_user_num = Integer()
    click_num = Integer()
    crawl_time = Date()

    class Meta:
        index = "geek_search"
        doc_type = "zhihu_ques"

if __name__ == "__main__":
    ArticleType.init()#produce mapping
    LagouType.init()
    ZhihuAnswerType.init()
    ZhihuQuestionType.init()