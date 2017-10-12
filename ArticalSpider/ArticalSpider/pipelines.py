# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
#I meet a erro, but solved though:http:
# //jingyan.baidu.com/album/3c48dd3473417de10ae3586c.html?picindex=4, just mark it in case it can be used in the future

#pipeline 接受爬来的数据可以处理，例如存入数据库
from scrapy.pipelines.images import ImagesPipeline
from models.es_types import ArticleType
from scrapy.exporters import JsonItemExporter
from twisted.enterprise import adbapi
from w3lib.html import remove_tags
#将mysql的操作变为异步化操作

import pymysql
import pymysql.cursors
import codecs
import json

class ArticalspiderPipeline(object):
    def process_item(self, item, spider):
        return item

class JsonWithEncodingPipeline(object):
    #自定义json文件的导出
    #jason only support number, string, boolean, array, dict and null, which is totally 6 data type
    def __init__(self):
        self.file = codecs.open('article.json', 'w', encoding="utf-8")
    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item
    def spider_closed(self, spider):
        self.file.close()

class JsonExporterPipline(object):
    #调用scrapy提供的json exporter， 导出json文件
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

#spider 解析速度超过数据库入库速度，会阻塞，用twist可以将插入异步化
#同步：一条不执行结束不会开始下一条；
class MysqlPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect('127.0.0.1', 'root', '123456', 'article_spider', charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into jobbole_article(title, url, create_date, fav_nums)
            VALUES (%s, %s, %s, %s)
        """
        self.cursor.execute(insert_sql, (item["title"], item["url"], item["create_date"], item["fav_nums"]))
        self.conn.commit()

#异步插入
class MysqlTwistedPipline(object):#TWIST 提供了异步容器

    def __init__(self, dbpool):#启动spider时候， 将dbpool传递
        self.dbpool = dbpool
    @classmethod
    def from_settings(cls, settings):#静态方法读取mysql配置
        dbparms = dict(
        host = settings["MYSQL_HOST"],
        db = settings["MYSQL_DBNAME"],
        user = settings["MYSQL_USER"],
        passwd = settings["MYSQL_PASSWORD"],
        charset = 'utf8',
        cursorclass = pymysql.cursors.DictCursor,
        use_unicode = True,
        )
        dbpool = adbapi.ConnectionPool("pymysql", **dbparms)
        #形参名前加两个*表示，参数在函数内部将被存放在以形式名为标识符的 dictionary 中，这时调用函数的方法则需要采用 arg1=value1,arg2=value2 这样的形式。
        #我把 *args 称作为数组参数，**kwargs 称作为字典参数
        return cls(dbpool)

    def process_item(self, item, spider):
        #使用twised 将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)#dbpool是一个容器，runinteraction在容器当中运行
        query.addErrback(self.handle_error, item, spider)

    def handle_error(self, failure, item, spider):
        print(failure)
        #处理异步插入异常

    def do_insert(self, cursor, item):
        #执行具体插入
        #根据不同的item，构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)



class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for ok, value in results:
                image_file_path = value["path"]
            item["front_image_path"] = image_file_path

        return item


class ElasticsearchPipeline(object):
    #read the data into ES
    def process_item(self, item, spider):
        #turn the item in the ES's item

        item.save_to_es()

        return item
