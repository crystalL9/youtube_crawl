import device_config_ultils
import time
import threading
import queue
from apps.api.crawler.youtube_crawler.youtube_crawler import YoutubeCrawlerTool
from apps.api.crawler.youtube_crawler.youtube_crawler import YoutubeCrawlerJob
from elasticsearch import Elasticsearch
from datetime import timedelta,datetime
import datetime as dt
from logger import Colorlog
from dotenv import load_dotenv
import os

queue_lock = threading.Lock()
tool_lock = threading.Lock()
list_driver = set()
queue_link_channel = queue.Queue()
queue_link_keyword = queue.Queue()
queue_link_update = queue.Queue()

def search_key_word(queue_link,tool,max_size_post,key_word_lists):
    local_device_config = device_config_ultils.get_local_device_config()
    username=local_device_config[0]['account']['username']
    password=local_device_config[0]['account']['password']
    if username == "null" or username=="" or username==None:
        username=None
    if password == "null" or password=="" or password==None:
        password=None
        max_size_post= local_device_config[0]['mode']['max_size_post']
        with tool_lock:
            try:
                    YoutubeCrawlerJob.get_link_search_key_word(
                    main_key_list= key_word_lists,
                    sub_key_list= None,
                    queue_link=queue_link,
                    tool=tool,
                    max_size_post=max_size_post)
            except Exception as e:
                    pass
def search_channel(queue_link,tool,list_channel,max_size_post):
        try:
            YoutubeCrawlerJob.get_link_search_channel(
                tool=tool,
                queue_link=queue_link,
                list_link_channels=list_channel,
                max_size_post=max_size_post
                )
        except Exception as e:
            time.sleep(15*60)
def crawl_videos(queue_link, tool,mode):
        while not queue_link.empty():
            try:
                link = queue_link.get(timeout=10)
                if 'shorts' not in link:
                    YoutubeCrawlerJob.crawl_information_video(
                        main_key=None,
                        sub_key=None,
                        link=link,
                        tool=tool,
                        mode=mode
                )
                
            except:
                print(f"{dt.datetime.now()} ERROR.")
                continue
def read_lines_from_file(url,mode):
    if mode ==1:
        try:
            with open('error/error_link.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    if mode ==2:
        try:
            with open(f'error/{str(url).split("/")[-1]}_error.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    return lines
def read_lines_from_file_done(url,mode):
    if mode ==1:
        try:
            with open('link_crawled/crawled.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    if mode ==2:
        try:
            with open(f'link_crawled/{str(url).split("/")[-1]}.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    return lines

def work_keyword(local_device_config):
    options_list=local_device_config[0]['listArgument']
    max_size_post=local_device_config[0]['mode']['max_size_post']
    username=local_device_config[0]['account']['username']
    password=local_device_config[0]['account']['password']
    key_word_lists=local_device_config[0]['mode']['keyword']

    if username == "null" or username=="" or username==None:
        username=None
    if password == "null" or password=="" or password==None:
        password=None
    try:
            for keyword in key_word_lists:
                tool3 = YoutubeCrawlerTool(options_list=options_list,username=username, password=password)      
                search_channel_long_thread = threading.Thread(target=search_key_word, args=(queue_link_keyword,tool3,max_size_post,[keyword]))
                search_channel_long_thread.start()
                search_channel_long_thread.join()
                tool3 = YoutubeCrawlerTool(options_list=options_list,username=username, password=password).kill_browser()
                if not queue_link_keyword.empty():
                    tool4  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                    crawl_thread4 = threading.Thread(target=crawl_videos, args=(queue_link_keyword, tool4,1))
                    crawl_thread4.start()
                    tool5  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                    crawl_thread5 = threading.Thread(target=crawl_videos, args=(queue_link_keyword, tool5,1))
                    crawl_thread5.start()
                    tool6  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                    crawl_thread6 = threading.Thread(target=crawl_videos, args=(queue_link_keyword, tool6,1))
                    crawl_thread6.start()
                    crawl_thread4.join()
                    crawl_thread5.join()
                    crawl_thread6.join()
                    YoutubeCrawlerTool(options_list=options_list,username=username, password=password).kill_browser()

    except Exception as e:
        print(e)
        pass   


def work1(local_device_config):
    options_list=local_device_config[0]['listArgument']
    channel_urls=local_device_config[0]['mode']['channel_link']
    max_size_post=local_device_config[0]['mode']['max_size_post']
    username=local_device_config[0]['account']['username']
    password=local_device_config[0]['account']['password']
    if username == "null" or username=="" or username==None:
        username=None
    if password == "null" or password=="" or password==None:
        password=None
    try:
        for channel in channel_urls:
            tool4  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password)      
            search_channel_long_thread2 = threading.Thread(target=search_channel, args=(queue_link_channel,tool4,[channel],max_size_post,))
            search_channel_long_thread2.start()
            search_channel_long_thread2.join()
            tool4 = YoutubeCrawlerTool(options_list=options_list,username=username, password=password).kill_browser()
            if not queue_link_channel.empty():
                tool6  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                crawl_thread6 = threading.Thread(target=crawl_videos, args=(queue_link_channel, tool6,2))
                crawl_thread6.start()
                tool7  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                crawl_thread7 = threading.Thread(target=crawl_videos, args=(queue_link_channel, tool7,2))
                crawl_thread7.start()
                tool8 = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
                crawl_thread8 = threading.Thread(target=crawl_videos, args=(queue_link_keyword, tool8,2))
                crawl_thread8.start()
                crawl_thread6.join()
                crawl_thread7.join()
                crawl_thread8.join()
                YoutubeCrawlerTool(options_list=options_list,username=username, password=password).kill_browser()
    except Exception as e:
        print(e)
        pass

pass

def get_link(queue_link,gte,lte):
    load_dotenv()
    es_address = os.getenv("Elasticsearch")
    es = Elasticsearch([f"{es_address}"])
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type.keyword": "youtube video"}},
                    {
                        "range": {
                            "created_time": {
                                "gte": gte,
                                "lte": lte,
                                "format": "MM/dd/yyyy HH:mm:ss"
                            }
                        }
                    }
                ]
            }
        },
        "size": 10000,
        "sort": [
            {
                "created_time": {
                    "order": "asc"
                }
            }
        ],
        "_source": ["link"]
    }

    result = es.search(index="posts", body=body)
    link=[]
    for hit in result['hits']['hits']:
        link.append(hit['_source']['link'])
        queue_link.put(hit['_source']['link'])
    es.close()
    print(f"{Colorlog.cyan_color}====== GET {len(link)}  from {gte} to {lte} for updating ====== {Colorlog.reset_color}")
   
def get_link_es(queue, time_start_update, range_date):
    current_time = dt.datetime.now().time()
    start_time = dt.datetime.strptime(time_start_update, "%H:%M").time()
    if current_time.hour == start_time.hour and (
        current_time.minute >= start_time.minute and
        current_time.minute <= start_time.minute + 1
    ):
        queue.queue.clear()
        time.sleep(3)
        print(f' Thời gian hiện tại là {time_start_update}. Bắt đầu thực hiện cập nhật')

        current_date = dt.datetime.now()
        current_date1 = dt.datetime.now() - dt.timedelta(days=int(range_date[0])-1)
        one_day_ago = current_date - dt.timedelta(days=int(range_date[0]))
        formatted_date = current_date1.strftime("%m/%d/%Y")
        one_day_ago_formatted_date = one_day_ago.strftime("%m/%d/%Y")
        six_day_ago = current_date - dt.timedelta(days=int(range_date[1]) - 1)
        seven_day_ago = current_date - dt.timedelta(days=int(range_date[1]))
        six_day_ago_formatted_date = six_day_ago.strftime("%m/%d/%Y")
        seven_day_ago_formatted_date = seven_day_ago.strftime("%m/%d/%Y")

        print(f"💻💻💻 Bắt đầu lấy link của ngày {one_day_ago_formatted_date} và ngày {seven_day_ago_formatted_date}")

        gte = f'{one_day_ago_formatted_date} 00:00:00'
        lte = f'{formatted_date} 00:00:00'
        try:
            get_link(queue_link=queue, gte=gte, lte=lte)
        except:
            pass

        time.sleep(5)

        gte = f'{seven_day_ago_formatted_date} 00:00:00'
        lte = f'{six_day_ago_formatted_date} 00:00:00'
        try:
            get_link(queue_link=queue, gte=gte, lte=lte)
        except:
            pass

        print(f" ☑ ☑ ☑ Đã lấy hết link của ngày {one_day_ago_formatted_date} và ngày {seven_day_ago_formatted_date}")


def update(local_device_config):
    options_list=local_device_config[0]['listArgument']
    time_start_upate=local_device_config[0]['mode']['start_time_run']
    range_date=local_device_config[0]['mode']['range_date']
    
    crawl_thread5 = threading.Thread(target=get_link_es, args=(queue_link_update,time_start_upate,range_date))
    crawl_thread5.start()
    crawl_thread5.join()
    if not queue_link_update.empty():
        username = local_device_config[0]['account']['username']
        password = local_device_config[0]['account']['password']
        if username == "null" or username=="" or username==None:
            username=None
        if password == "null" or password=="" or password==None:
            password=None
        tool6  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
        crawl_thread6 = threading.Thread(target=crawl_videos, args=(queue_link_update, tool6,3))
        crawl_thread6.start()
        tool7  = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
        crawl_thread7 = threading.Thread(target=crawl_videos, args=(queue_link_update, tool7,3))
        crawl_thread7.start()
        tool8 = YoutubeCrawlerTool(options_list=options_list,username=username, password=password) 
        crawl_thread8 = threading.Thread(target=crawl_videos, args=(queue_link_update, tool8,3))
        crawl_thread8.start()
        crawl_thread6.join()
        crawl_thread7.join()
        crawl_thread8.join()
        YoutubeCrawlerTool(options_list=options_list,username=username, password=password).kill_browser()
