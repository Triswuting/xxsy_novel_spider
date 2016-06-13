#!/usr/bin/python
import json
import re
import math
import urllib.request
import pymongo
import time
import pymongo
from threading import Thread
from queue import Queue

#import my_pymongo

from bs4 import BeautifulSoup


class my_pymongo():
    def __init__(self,host,port,database):
        self.host = host
        self.port = port
        self.database = database

    def get_connected(self):
        client = pymongo.MongoClient(self.host,self.port)
        db = client[self.database]
        return client,db

    def get_collection(self,db,collection):
        if not db[collection]:
            db.create_collection(collection)
        coll = db[collection]
        return coll

    def insert_one_doc(self,coll,doc):
        info_id = coll.insert(doc)
        info_count = coll.count()
        return info_id,info_count

    def get_index(self,coll,key,direction):
        coll.create_index(key,pymongo.ASCENDING)

    def find(self,coll,filter=None,projection=None,limit=0):
        return coll.find(filter,projection,limit)

class my_thread(Thread): #线程类，请求并存储数据
    def __init__(self,mydb,coll,worker_queue):
        Thread.__init__(self)
        self.worker_queue = worker_queue
        self.mydb = mydb
        self.coll = coll

    def run(self):
        while True:
            url = self.worker_queue.get()
            self.write_data(url)
            self.worker_queue.task_done()

    def write_data(self,url):
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36',
            'Referer': 'https://www.hao123.com/link/https/?key=http%3A%2F%2Fwww.xxsy.net%2F&'
        }
        req_sec = urllib.request.Request(url, headers=headers)
        page_sec = urllib.request.urlopen(req_sec).read()
        x = json.loads(page_sec.decode())['booklist']
        info_id,info_count = self.mydb.insert_one_doc(self.coll,x)

class xxsy_novel_spider:
    def __init__(self):
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36'
        self.headers = {'User-Agent': self.user_agent,
                        'Referer':'https://www.hao123.com/link/https/?key=http%3A%2F%2Fwww.xxsy.net%2F&'}
        self.host = 'http://www.xxsy.net/'

    """自动生成查询类的URL"""
    def get_book_list(self,mydb,db):
    # 通过下列参数组成url
    #形成主线程任务队列
        search_type = [1,2,3,6,7,9,12, 13, 14, 15, 16, 17]  # 小说类型参数
        search_page_count = 80  # 结果列表每页小说个数
        search_rand = str(math.ceil(time.time())) #'1464074348875'  # time.time()  #从1997.1.1至今经过的毫秒数
        coll = mydb.get_collection(db, 'book_info')
        worker_queue = Queue()
        for j in range(1,5,1):
            thread = my_thread(mydb,coll,worker_queue)
            thread.daemon = True
            thread.start()
        for i in search_type:
            url_list = []
            search_page_no = 1  # 搜索结果页码
            search_url = self.host + 'search.aspx?q=' + '&cp=' + str(i) + '&sort=9' + '&rn=' + str(
                search_page_count) + '&pn=' + str(search_page_no) + '&rand=' + search_rand
            req = urllib.request.Request(search_url, headers=self.headers)
            page = urllib.request.urlopen(req).read()
            total = json.loads(page.decode())['total']
            print('total:',total)
            if total < 80:
                search_page_range = 1
            else:
                search_page_range = math.ceil(total / search_page_count)
            for search_page_no in range(1, search_page_range, 1):
                if search_page_no == search_page_range:
                    search_page_count = total % 80
                search_url = self.host + '/search.aspx?q=' + '&cp=' + str(i) + '&sort=9' + '&rn=' + str(
                    search_page_count) + '&pn=' + str(search_page_no) + '&rand=' + search_rand
                worker_queue.put(search_url)
        worker_queue.join()
        return url_list

    def get_novel_menu(self,book_id):#得到小说目录
        url='http://www.xxsy.net/books/'+str(book_id)+'/default.html'
        req = urllib.request.Request(url,headers = self.headers)
        page = urllib.request.urlopen(req).read()
        soup = BeautifulSoup(page)
        menu = []
        all_text = soup.find_all('a')   # 提取记载有小说章节名和链接地址的模块
        regex=re.compile(u'\u7b2c.+\u7ae0')          # 中文正则匹配第..章，去除不必要的链接
        for title in all_text:
            if re.findall(regex,title.text):
                name = title.text
                chapter_id = int(str(title['href']).replace('.html', ''))
                x = [name,chapter_id,self.host+'books/'+str(book_id)+'/'+title['href']]
                menu.append(x)       # 把记载有小说章节名和链接地址的列表插入列表中
                print(x)
        return menu

    def get_chapter(self,name,url):#获取每章节内容
        """Get every chapter in menu"""
        req = urllib.request.Request(url,headers = self.headers)
        html=urllib.request.urlopen(url).read()
        soup=BeautifulSoup(html)
        all_text = soup.find_all('div',id={'zjcontentdiv'})
        content = BeautifulSoup(str(all_text[0])).get_text()
        #print(content)
        return content

    def save_whole_book(self,book_id,book_name,mydb,db):
        menu = xns.get_novel_menu(book_id)
        coll = mydb.get_collection(db,'book_chapter')
        for i in menu:
            content = xns.get_chapter(i[0], i[2])
            x = {'bookid': book_id, 'chapterid': i[1], 'chaptername': i[0]}
            x['chaptercontent'] = content
            mydb.insert_one_doc(coll, x)

if __name__=='__main__':
    xns = xxsy_novel_spider()
    mydb = my_pymongo('localhost', 27017, 'wt_book')  # 连接数据库
    client,db = mydb.get_connected()
    start = time.localtime(time.time())
    print('start:',start)
    url_list = xns.get_book_list(mydb,db)
    end = time.localtime(time.time())
    print('end:',end)
    coll = mydb.get_collection(db,'book_info')
    result_set = mydb.find(coll,{'vip':0},{'_id':0,'bookid':1,'bookname':1})
    start = time.localtime(time.time())
    print(start)
    for i in result_set.limit(1):
        print(i)
        xns.save_whole_book(i['bookid'],i['bookname'],mydb,db)
    client.kill_cursors(result_set.cursor_id,result_set.address)
    end = time.localtime(time.time())
    print(end)
