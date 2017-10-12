# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from urllib import parse
from items import JobBoleArticleItem, ArticleItemLoader
from utils.common import get_md5

class JobboleSpider(scrapy.Spider):
    name = "jobbole"
    allowed_domains = ["blog.jobbole.com"]
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        """
        获取文章下一页的url并交给scrapy进行下载后进行解析
        1 获取文章下一页url并交给scrapy进行下载， 下载完成后交给parse函数
        2并交给解析函数进行具体字段的解析
        """
        #解析列表页面中所有文章URL， 并交给scrapy进行解析
        post_nodes = response.css("#archive .floated-thumb .post-thumb a")
        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first("")
            post_url = post_node.css("::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url), meta={"front_image_url":image_url}, callback=self.parse_detail)
        #提取下一页并交给scrapy进行下载
        next_url = response.css(".next.page-numbers::attr(href)").extract_first("")
        if next_url:
            #yield Request(url=parse.urljoin(response.url, post_url), callback=self.parse)
            yield Request(next_url, callback=self.parse)



    def parse_detail(self, response):
        article_item = JobBoleArticleItem()
        """
        #通过xpath选取字段
        title = response.xpath('//div[@class="entry-header"]/h1/text()').extract()
        #re2_selector = response.xpath('//*[@id ="post-110811"]/div[1]/h1/text()')
        create_date = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()[0].strip().replace("·","").strip()

        praise_nums = int(response.xpath("//span[contains(@class,'vote-post-up')]/h10/text()").extract()[0])
        fav_nums = response.xpath("//span[contains(@class,'bookmark-btn')]/text()").extract()[0]
        match_re = re.match(r".*(\d+).*", fav_nums)
        if match_re:
            fav_nums = int(match_re.group(1))
        else:
            fav_nums = 0
        comment_nums = response.xpath("//a[@href='#article-comment']/span/text()").extract()[0]
        match_re = re.match(r".*(\d+).*", comment_nums)
        if match_re:
            comment_nums = int(match_re.group(1))
        else:
            comment_nums = 0
        content = response.xpath("//div[@class='entry']").extract()[0]
        comment_date = response.xpath("//p[@class='entry-meta-hide-on-mobile']/text()").extract()
        tag_list = response.xpath("//p[@class='entry-meta-hide-on-mobile']/a/text()").extract()
        tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        tags = ",".join(tag_list)
        print(tags)
        """
        #通过css选择器提取字段
        front_image_url = response.meta.get("front_image_url", "") #文章封面图
        # title = response.css(".entry-header h1::text").extract_first()
        # create_date = response.css("p.entry-meta-hide-on-mobile::text").extract()[0].strip().replace("·","").strip()
        # #print(create_date)
        # praise_nums = response.css(".vote-post-up h10::text").extract()[0].strip()
        # match_re = re.match(".*?(\d+).*", praise_nums)
        # if match_re:
        #     praise_nums = int(match_re.group(1))
        # else:
        #     praise_nums = 0
        # fav_nums = response.css("span.bookmark-btn ::text").extract()[0].strip()
        # match_re = re.match(".*?(\d+).*", fav_nums)
        # if match_re:
        #     fav_nums = int(match_re.group(1))
        # else:
        #     fav_nums = 0
        # comment_nums = response.css("a[href='#article-comment'] span::text").extract()[0].strip()
        # match_re = re.match(".*?(\d+).*", comment_nums)
        # if match_re:
        #     comment_nums = int(match_re.group(1))
        # else:
        #     comment_nums = 0
        # content = response.css("div.entry").extract()[0]
        # tag_list = response.css("p.entry-meta-hide-on-mobile a::text").extract()
        # tag_list = [element for element in tag_list if not element.strip().endswith("评论")]
        # tags = ",".join(tag_list)
        #
        # article_item["url_object_id"] = get_md5(response.url)
        # article_item["title"] = title
        # article_item["url"] = response.url
        # try:
        #     create_date = datetime.datetime.strptime(create_date, "%Y/%m/%d").date()
        # except Exception as e:
        #     create_date = datetime.datetime.now().date()
        # article_item["create_date"] = create_date
        # article_item["front_image_url"] = [front_image_url]
        # article_item["praise_nums"] = praise_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["tags"] = tags
        # article_item["content"] = content

        #通过item loader加载item；
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_css("title", ".entry-header h1::text")
        #item_loader.add_xpath()
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_css("create_date", "p.entry-meta-hide-on-mobile::text")
        item_loader.add_css("praise_nums", ".vote-post-up h10::text")
        item_loader.add_css("comment_nums", "a[href='#article-comment'] span::text")
        item_loader.add_css("fav_nums", "span.bookmark-btn ::text")
        item_loader.add_css("tags", "p.entry-meta-hide-on-mobile a::text")
        item_loader.add_css("content", "div.entry")
        article_item = item_loader.load_item()

        yield article_item


