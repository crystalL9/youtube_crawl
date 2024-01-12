import time
from pytube import YouTube
from selenium.webdriver.common.by import By
from apps.api.crawler.utils.crawler_logger import CrawlerLogger
from .utils import (
    DetailCrawler,
    HomePageCrawler,
    YoutubeUtil,
    StringHandler,
    GChromeDriver
)
from logger import *
from call_api import get_links
YOUTUBE_HOMEPAGE_URL = "https://www.youtube.com"
BROWSER_LANGUAGE = "en"
HASHTAG_SEARCH_URL = "https://www.youtube.com/hashtag"

crawler_logger = CrawlerLogger()


class YoutubeCrawlerTool:
    def __init__(self, options_list,username=None, password=None):
        self.driver = GChromeDriver.init_driver(options_list,username, password)
        self.detail_crawler = DetailCrawler(self.driver)
        self.string_handler = StringHandler
        self.homepage_crawler = HomePageCrawler(self.driver)
        self.util = YoutubeUtil(self.driver)
        username = username
        password = password
    def kill_browser(self):
        self.driver.quit()    # tab chrome để lấy link search theo keyword
    def get_link_search_key_word(self,sub_key_list,main_key_list,queue_link,max_size_post):
            # lấy link đã crawl từ file local
            #crawled_video_links = self.read_file_lines('link_crawled/crawled.txt')

            # lấy link đã crawl từ db
            crawled_video_links=get_links("youtube_video","crawled")['links']
            if sub_key_list!=None:
                for mainkey in main_key_list:
                    for subkey in sub_key_list:
                        # kết quả tìm kiếm được sắp xếp theo ngày đăng
                        search_url = f'https://www.youtube.com/results?search_query="{mainkey}""{subkey}"&sp=CAI%253D'
                        self.driver.get(search_url)
                        time.sleep(3)
                        video_links=self.extract_link_from_page(crawled_link=crawled_video_links,queue_link=queue_link,max_size_post=max_size_post)
                        if len(video_links)!=0:
                            for link in reversed(video_links):
                                queue_link.put(link)
                print("*****************")
                self.driver.get('about:blank')  

            elif sub_key_list==None:
                    for mainkey in main_key_list:
                        print(f'================= Tìm kiếm với từ khóa "{mainkey}" ================= ')
                        # kết quả tìm kiếm được sắp xếp theo ngày đăng
                        search_url = f'https://www.youtube.com/results?search_query="{mainkey}"&sp=CAI%253D'
                        self.driver.get(search_url)
                        time.sleep(3)
                        video_links=self.extract_link_from_page(crawled_link=crawled_video_links,queue_link=queue_link,max_size_post=max_size_post)
                        if len(video_links)!=0:
                            for link in reversed(video_links):
                                queue_link.put(link)
                    print("*****************")
                    self.driver.get('about:blank')
            
    # tab chrome để lấy link search theo channel
    def get_link_search_channel(self,queue_link,list_link_channels,max_size_post):
            for link_channel in list_link_channels:
                id_channel=link_channel.split('/')[-1]
                try:
                    # lấy link đã crawl từ file local
                    #crawled_video_links = self.read_file_lines(f'link_crawled/{id_channel}.txt')

                    # lấy link đã crawl từ db
                    crawled_video_links=get_links('youtube_video',id_channel)['links']
                except:
                    crawled_video_links = []

                try:
                    self.driver.get(f'{link_channel}/videos')
                    time.sleep(3)
                    video_links=self.extract_link_from_channel(crawled_link=crawled_video_links,queue_link=queue_link,max_size_post=max_size_post)
                    if len(video_links)!=0:
                        for link in reversed(video_links):
                            queue_link.put(link)
                    time.sleep(3)
                except:
                    pass
                try:
                    self.driver.get(f'{link_channel}/streams')
                    time.sleep(3)
                    video_links=self.extract_link_from_channel(crawled_link=crawled_video_links,queue_link=queue_link,max_size_post=max_size_post)
                    if len(video_links)!=0:
                        for link in reversed(video_links):
                            queue_link.put(link)
                    time.sleep(3)
                except:
                    pass
            print("******** Search hết các video hiện tại, tạm dừng chờ video mới *********")
            self.driver.get('about:blank')

   # Đọc file ra list
    def read_file_lines(self,file_path):
        lines = []
        with open(file_path, 'r') as file:
            for line in file:
                lines.append(line.strip())
        return lines
    
    # Xử lý để lấy link gốc
    def excute_link(self,link):
        if '&' in link:
            link = link.split('&')[0]
        return link
    # Lưu các link đã crawl ra file
    def save_array_to_txt(self, arr, file_path):
        with open(file_path, 'a') as file:
            for element in arr:
                file.write(str(element))
                file.write('\n')

    def crawl_information_video(self,video_link, main_key, sub_key,mode):
        crawled_data_list = []
        try:
            crawl_data = self.detail_crawler.run(
                video_url=video_link,
                main_key=main_key,
                sub_key=sub_key,
                mode=mode
            )
            if crawl_data:
                video_detail, comments = crawl_data
                crawled_data_list.append(video_detail)
                crawled_data_list.extend(comments)
        except Exception as e:
            crawler_logger.error(str(e))
        return crawled_data_list

    def crawl_information_video_update(self,video_link, main_key, sub_key,mode,check):
        crawled_data_list = []
        try:
            crawl_data = self.detail_crawler.run_update(
                video_url=video_link,
                main_key=main_key,
                sub_key=sub_key,
                mode=mode,
                check=check
            )
            if crawl_data:
                video_detail, comments = crawl_data
                crawled_data_list.append(video_detail)
                crawled_data_list.extend(comments)
        except Exception as e:
            crawler_logger.error(str(e))
        return crawled_data_list

    def report_video(self, video_link):
        self.driver.maximize_window()
        self.driver.get(video_link)
        time.sleep(2)
        self.util.report()

    def _scrape_videos_by_keyword(self, keyword, interact_option):
        self.driver.minimize_window()
        format_keyword = "+".join(keyword.split())

        # kết quả tìm kiếm được sắp xếp theo ngày đăng
        search_url = f"https://www.youtube.com/results?search_query={format_keyword}&sp=CAI%253D"
        self.driver.get(search_url)
        #self.util.recently_click()
        return self._crawled_data_in_search_screen(interact_option,
                                                   keyword)

    
    
    
    def save_link(self, txt, file_path):
        with open(file_path, 'a') as file:
                file.write(txt)
                file.write('\n')
    

    def link_to_id(self,link):
        yt = YouTube(link)
        video_id = yt.video_id
        return video_id
        
    def extract_link_from_page(self,crawled_link,queue_link,max_size_post):
        video_links=[]
        while True:
            before_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")

            time.sleep(3)
            after_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            video_title_elements = self.driver.find_elements(By.XPATH, '//a[@id="video-title"]')
            reversed_elements = video_title_elements
            for element in reversed_elements:
                link= self.excute_link(element.get_attribute("href"))
                id=self.link_to_id(link)
                if id in video_links:
                    pass
                elif id in crawled_link:
                        return video_links
                else:
                        video_links.append(link)
            if after_scroll_height == before_scroll_height:
                return video_links
            if max_size_post > 0 :
                if len(video_links) >= int(max_size_post):
                    return video_links
                else:
                    pass

    def extract_link_from_channel(self,crawled_link,queue_link,max_size_post):
        video_links=[]
        id_video=[]
        while True:
            before_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(3)
            after_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            video_title_elements = self.driver.find_elements(By.XPATH, '//*[@id="thumbnail"]/ytd-thumbnail/a')
            for element in video_title_elements:
                link= self.excute_link(element.get_attribute("href"))
                if 'shorts' in str(link):
                    continue
                else:
                    id=self.link_to_id(link)
                    if id in id_video:
                        pass
                    elif id in crawled_link:
                            return video_links
                    else:
                            id_video.append(id)
                            video_links.append(link)
                        
            if after_scroll_height == before_scroll_height:
                return video_links
            if max_size_post > 0 :
                if len(video_links) >= int(max_size_post):
                    return video_links
                else:
                    pass
            
    def extract_link_from_channel_long(self,crawled_link,queue_link):
        video_links=[]
        while True:
            if len(video_links)>500:
                return video_links
            before_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(5)
            after_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            video_title_elements = self.driver.find_elements(By.XPATH, '//*[@id="thumbnail"]/ytd-thumbnail/a')
            for element in video_title_elements:
                link= self.excute_link(element.get_attribute("href"))
                if link in video_links:
                    pass
                elif link in crawled_link:
                        return video_links
                else:
                        video_links.append(link)
            if after_scroll_height == before_scroll_height:
                return video_links
            
    def scroll_down(self):
        while True:
            before_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(5)
            after_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")

            if after_scroll_height == before_scroll_height:
                break

class YoutubeCrawlerJob:
    def __init__(self):
        pass

    def __getitem__(self, key):
        if hasattr(self, key) and callable(getattr(self, key)):
            return lambda *args, **kwargs: getattr(self, key)(*args, **kwargs)
        else:
            raise KeyError(f"Method '{key}' not found")
    
    @staticmethod
    def get_link_search_key_word(sub_key_list,main_key_list,queue_link,tool,max_size_post):
        try:
            tool.get_link_search_key_word(main_key_list=main_key_list,sub_key_list=sub_key_list,queue_link=queue_link,max_size_post=max_size_post)
            tool.driver.get('about:blank')
        except:
            tool.driver.quit()
        
    @staticmethod
    def crawl_information_video(sub_key,main_key,link,tool,mode):
        try:
            tool.crawl_information_video(video_link=link, main_key=main_key, sub_key=sub_key,mode=mode)
            tool.driver.get('about:blank')
        except:
            tool.driver.quit()
    @staticmethod
    def crawl_information_video_update(sub_key,main_key,link,tool,mode,check):
        try:
            tool.crawl_information_video_update(video_link=link, main_key=main_key, sub_key=sub_key,mode=mode,check=check)
            tool.driver.get('about:blank')
        except:
            tool.driver.quit()
    
    @staticmethod
    def get_link_search_channel(queue_link,list_link_channels,tool,max_size_post):
        try:
            tool.get_link_search_channel(queue_link=queue_link,list_link_channels=list_link_channels,max_size_post=max_size_post)
            tool.driver.get('about:blank')
        except:
            tool.driver.quit()



