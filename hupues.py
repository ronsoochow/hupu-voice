# -*- coding:UTF-8 -*-
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests

"""
类说明:elastic的操作
Parameters:
    无
Returns:
    无
Modify:
    2018-11-09
"""
class ElasticObj:
    def __init__(self, index_name,index_type,ip ="127.0.0.1"):
        '''

        :param index_name: 索引名称
        :param index_type: 索引类型
        '''
        self.index_name =index_name
        self.index_type = index_type
        # 无用户名密码状态
        #self.es = Elasticsearch([ip])
        #用户名密码状态
        self.es = Elasticsearch([ip],http_auth=('elastic', 'changeme'),port=9200)

    def Get_Data_Id(self,id):
        res = self.es.get(index=self.index_name, doc_type=self.index_type,id=id)
        print(res)
        print(res['_source'])


    def Index_Data(self,item):
        '''
        数据存储到es
        :return:
        '''
        # item = {   
        #         "id":"1",
        #         "title": "第一条测试的新闻标题",
        #         "content": "第一条测试的新闻内容"
        #      }
        res = self.es.index(index=self.index_name, doc_type=self.index_type, body=item,id=item["id"])
        print(res)
        print(res['created'])

    def Delete_Index_Data(self,id):
        '''
        删除索引中的一条
        :param id:
        :return:
        '''
        res = self.es.delete(index=self.index_name, doc_type=self.index_type, id=id)
        print(res)


"""
类说明:获取虎扑voice的新闻
Parameters:
    无
Returns:
    无
Modify:
    2018-11-09
"""
class VoiceNews:
    def __init__(self):
        self.tagUrl="https://voice.hupu.com/nba/tag/"
        self.newsUrl="https://voice.hupu.com/nba/"

    def GetNewsLinks(self,teamId,pageNum):
        urls=[]
        if pageNum>100 or pageNum<1:
            pass
        else:
            tag=self.tagUrl+teamId+"-"+str(pageNum)+".html"
            req=requests.get(url=tag)
            html=req.text
            div_bf=BeautifulSoup(html,'html.parser')
            divTitle=div_bf.find_all('div',class_='list-content')
            for each in divTitle:
                spanTitle_bf=BeautifulSoup(str(each),'html.parser')
                spanTitle=spanTitle_bf.find_all('span',class_='n1')
                aTitle=spanTitle[0].find_all('a')
                urls.append(aTitle[0].get('href'))
        return urls        

    def GetNewsContent(self,url,newsId,teamName):
        target=url
        req=requests.get(url=target)
        html=req.text
        div_bf=BeautifulSoup(html,'html.parser')
        divTitle=div_bf.find_all('h1',class_='headline')
        title = divTitle[0].text
        divContent=div_bf.find_all('div',class_='artical-content')
        content=divContent[0].text
        spanPubTime=div_bf.find('span',id='pubtime_baidu')
        pubTime=spanPubTime.text
        item={"id":newsId.strip(),"title":title.strip(),"team":teamName.strip(),"pubtime":pubTime.strip(),"content":content.strip()}
        return item




# voice新闻列表，雷霆:2987
# typeList={"thunder":"2987"}
typeList={"thunder":"2987","lakers":"846"}

# es的index设置为hupunews,type为voice
esObj=ElasticObj("hupunews","voice")

# 只看前n-1页的列表
endPage=3

newsObj=VoiceNews()
for k,v in typeList.items():   
    urls=[] 
    for i in range(1,endPage):
        urls=newsObj.GetNewsLinks(v,1)
    # print(k+":"+str(urls)) 
    print("=========="+k+"==========")    
    for url in urls:
        newsId=url.replace(newsObj.newsUrl,'').replace('.html','')
        # print(newsId)
        singleNews=newsObj.GetNewsContent(url,newsId,k)
        esObj.Index_Data(singleNews)
    



