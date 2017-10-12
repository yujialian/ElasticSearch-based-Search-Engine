import json
import redis
from django.shortcuts import render
from django.views.generic.base import View
from search.models import ArticleType
from django.http import HttpResponse
from elasticsearch import Elasticsearch
from datetime import datetime

client = Elasticsearch(hosts=["127.0.0.1"])
redis_cli = redis.StrictRedis()
# Create your views here.

class IndexView(View):
    #front page
    def get(self, request):
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        return render(request, "index.html", {"topn_search": topn_search})

class SearchSuggest(View):
    def get(self, request):
        #suggest logic
        key_words = request.GET.get('s', '')#get keyword
        re_datas = []
        if key_words:
            s = ArticleType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy":{
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                if "title" in source:
                    if source["title"]:
                        re_datas.append(source["title"])

        return HttpResponse(json.dumps(re_datas), content_type="application/json")

class SearchView(View):
    def get(self, request):
        key_words = request.GET.get("q", "")
        s_type = request.GET.get("s_type", "")
        #print("s_type is : ", s_type)
        redis_cli.zincrby("search_keywords_set", key_words)#finish key_words weight + 1

        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)

        page = request.GET.get("p", 1)
        try:
            page = int(page)
        except:
            page = 1

        jobbole_count = redis_cli.get("jobbole_count")
        ans_count = redis_cli.get("ans_count")
        job_count = redis_cli.get("job_count")
        question_count = redis_cli.get("question_count")

        start_time = datetime.now()

        if s_type == "article":
            response = client.search(
                index="geek_search",
                doc_type='article',
                body={
                    "query":{
                        "multi_match":{
                            "query":key_words,
                            "fields":["tags", "title", "content"]
                        }
                    },
                    "from":(page-1)*10,
                    "size":10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {},
                        }
                    }
                }
            )
        elif s_type == "question":
            response = client.search(
                index="geek_search",
                doc_type='zhihu_ques',
                body={
                    "query":{
                        "multi_match":{
                            "query":key_words,
                            "fields":["topics", "title", "content"]
                        }
                    },
                    "from":(page-1)*10,
                    "size":10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {},
                        }
                    }
                }
            )

        else:
            response = client.search(
                index="geek_search",
                doc_type='job',
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["tags", "title", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {},
                        }
                    }
                }
            )
        end_time = datetime.now()

        last_seconds = (end_time - start_time).total_seconds()
        total_nums = response["hits"]["total"] #total number of results after multi_match
        if (page%10) > 0:
            page_nums = int(total_nums/10) + 1
        else:
            page_nums = int(total_nums/10)
        hit_list = []

        for hit in response["hits"]["hits"]:

            hit_dict = {}
            if s_type == "article":
                if "highlight" in hit:
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    elif "title" in hit["_source"]:
                        hit_dict["title"] = hit["_source"]["title"]
                else:
                    if "title" in hit["_source"]:
                        hit_dict["title"] = hit["_source"]["title"]
                    else:
                        continue
                if "highlight" in hit:
                    if "content" in hit["highlight"]:
                        hit_dict["content"] = "".join(hit["highlight"]["content"])[:500]
                    else:
                        hit_dict["content"] = ["_source"]["content"][:500]
                else:
                    if "content" in hit["_source"]:
                        hit_dict["content"] = hit["_source"]["content"][:500]
                    else:
                        continue
                if "create_date" in hit["_source"]:
                    hit_dict["create_date"] = hit["_source"]["create_date"]
                else:
                    hit_dict["create_date"] = datetime.now()

            if s_type == "question":
                if "title" in hit["highlight"]:
                    hit_dict["title"] = "".join(hit["highlight"]["title"])
                else:
                    hit_dict["title"] = "".join(hit["_source"]["title"])
                if "content" in hit["highlight"]:
                    hit_dict["content"] = "".join(hit["highlight"]["content"])[:500]
                else:
                    hit_dict["content"] = hit["_source"]["content"]
                hit_dict["create_date"] = hit["_source"]["crawl_time"]

            if s_type == "job":
                if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                else:
                    hit_dict["title"] = hit["_source"]["title"]
                if "job_desc" in hit["_source"]:
                    hit_dict["content"] = hit["_source"]["job_desc"][:200]
                else:
                    hit_dict["content"] = hit["_source"]["job_advantage"]

            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]

            hit_list.append(hit_dict)

            #window.location.href = search_url + '?q=' + key_words + "&s_type=" +$(".searchItem.current").attr('data-type')
        #+'&p=' + page_id

        return render(request, "result.html", {"page":page,
                                               "all_hits":hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums":page_nums,
                                               "last_seconds":last_seconds,
                                               "jobbole_count":jobbole_count,
                                               "ans_count": ans_count,
                                               "question_count": question_count,
                                               "job_count": job_count,
                                               "topn_search": topn_search,
                                               "s_type": s_type})