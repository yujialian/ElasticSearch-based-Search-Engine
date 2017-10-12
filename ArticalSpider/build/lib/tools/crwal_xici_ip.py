import requests
from scrapy.selector import Selector
import pymysql.cursors


conn = pymysql.connect(host = "127.0.0.1", user = "root", passwd = "123456", db = "article_spider", charset="utf8")
cursor = conn.cursor()#用于执行句柄

def crawl_ips():
    #爬取西刺的免费ip代理
    headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    for i in range(1568):
        re=requests.get("http://www.xicidaili.com/nn/{0}".format(i), headers=headers)

        selector = Selector(text=re.text)
        all_trs = selector.css("#ip_list tr")#<class 'scrapy.selector.unified.SelectorList'>

        ip_list = []
        for tr in all_trs[1:]:#tr is selector 进行嵌套选择器
            speed_str = tr.css(".bar::attr(title)").extract()[0]
            if speed_str:#<class 'scrapy.selector.unified.SelectorList'>
                speed = float(speed_str.split("秒")[0])
            all_texts = tr.css("td::text").extract()

            ip = all_texts[0]
            port = all_texts[1]
            proxy_type = all_texts[5]

            ip_list.append((ip, port, proxy_type, speed))
            #print(all_texts)
        for ip_info in ip_list:

            cursor.execute(
                """
                insert into proxy_ip(ip, port, speed, proxy_type) VALUES(%s, %s, %s, %s)  ON DUPLICATE KEY UPDATE ip=VALUES(ip),
                port=VALUES(port), speed=VALUES(speed),proxy_type=VALUES(proxy_type)
                """
                ,
                (ip_info[0], ip_info[1], ip_info[3], ip_info[2])
            )
            conn.commit()

class GetIP(object):
    def delete_ip(self, ip):
        delete_sql="""
        delete from proxy_ip where ip='{0}'
        """.format(ip)
        cursor.execute(delete_sql)
        conn.commit()
        return True

    def judge_ip(self, ip, port):
        #判断ip是否可用
        http_url = "http://www.wvu.com"
        proxy_url = "http://{0}:{1}".format(ip, port)
        try:
            proxy_dict = {
                "http":proxy_url
            }
            requests.get(http_url, proxies=proxy_dict)
            return True
        except Exception as e:
            print("invalid IP and port")
            self.delete_ip(ip)
            return False
        else:
            code = response.status_code
            if code >= 200 and code < 300:
                print("effective ip!")
                return True
            else:
                print("invalid ip and port!")
                self.delete_ip(ip)
                return False

    def get_random_ip(self):
        #从数据库中随机获取一个可用的ip
        random_sql = """
            SELECT ip, port FROM proxy_ip
            ORDER BY RAND()
            LIMIT 1
        """
        result = cursor.execute(random_sql)
        for ip_info in cursor.fetchall():
            ip = ip_info[0]
            port = ip_info[1]
            judge_re = self.judge_ip(ip, port)
            if judge_re:
                return "http://{0}:{1}".format(ip, port)
            else:
                return self.get_random_ip()
#crawl_ips()
if __name__ == "__main__":
    get_ip = GetIP()
    get_ip.get_random_ip()
