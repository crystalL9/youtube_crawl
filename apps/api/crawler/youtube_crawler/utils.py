import json
import re
import time
import dateparser
import requests
import datetime
import json
from pytube import YouTube
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from urllib.parse import urlparse, parse_qs
from apps.api.crawler.youtube_crawler.kafka_ncs_temp import push_kafka,push_kafka_update
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from apps.api.crawler.utils.crawler_logger import CrawlerLogger
from .result import Post
from jsonpath_ng import parse
from selenium.webdriver.support.ui import WebDriverWait
from call_api import insert
from logger import Colorlog
import device_config_ultils
from selenium.webdriver.chrome.service import Service

YOUTUBE_HOMEPAGE_URL = "https://www.youtube.com"
LOGIN_URL = (
    "https://accounts.google.com/InteractiveLogin/identifier?continue=https%3A%2F%2Fwww.youtube.com"
    "%2Fsignin%3Faction_handle_signin%3Dtrue%26app%3Ddesktop%26hl%3Dvi%26next%3Dhttps%253A%252F%252F"
    "www.youtube.com%252F&ec=65620&hl=vi&passive=true&service=youtube&uilel=3&ifkv=AXo7B7U1ikC89cjgyX"
    "oCLIqMxweO4G9w_-_ZilwLmu3PfZDXlw0JcdQP3wM4Y9lEhwb_BwgAEtO2&flowName=GlifWebSignIn&flowEntry=ServiceLogin"
)
crawler_logger = CrawlerLogger()

# Update 26/09/2023:
# - Sử dụng RE để lấy thông tin bình luận
##########################################


#  Sắp xếp bình luận 
SORT_BY_POPULAR = 0 # Sắp xếp theo " Bình luận hàng đầu"
SORT_BY_RECENT = 1 # Sắp xếp theo " Mới nhất sắp xếp trước"
# Chọn 1 trong 2 chế độ sắp xếp để lấy được hết thông tin commnet
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
YT_CFG_RE = r'ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;'
YT_INITIAL_DATA_RE = r'(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=\s*({.+?})\s*;\s*(?:var\s+meta|</script|\n)'
YT_HIDDEN_INPUT_RE = r'<input\s+type="hidden"\s+name="([A-Za-z0-9_]+)"\s+value="([A-Za-z0-9_\-\.]*)"\s*(?:required|)\s*>'
class InteractOption:
    def __init__(self, like_mode=False, report_mode=False, comment_mode=False, comment_sample_list=None):
        self.like_mode = like_mode
        self.report_mode = report_mode
        self.comment_mode = comment_mode
        self.comment_sample_list = comment_sample_list


class HomePageCrawler:
    def __init__(self, driver):
        self.driver = driver

    def get_all_relate_video_info(self):
        self.driver.get(YOUTUBE_HOMEPAGE_URL)
        time.sleep(5)
        for _ in range(2):
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(3)

        video_info_list = []
        video_thumbnails = self.driver.find_elements(
            "css selector", "a#video-title-link"
        )

        for thumbnail in video_thumbnails:
            video_info = {
                "link": thumbnail.get_attribute("href"),
                "title": thumbnail.get_attribute("title"),
            }
            video_info_list.append(video_info)
        return video_info_list


class DetailCrawler:
    
    def __init__(self, driver):
        self.driver = driver
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        self.session.cookies.set('CONSENT', 'YES+cb', domain='.youtube.com')
        self.util = YoutubeUtil(self.driver)

    def scroll_down_action(self, amount):
        body = self.driver.find_element("tag name", "body")
        for _ in range(amount):
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(1)

    def search_words_in_string(self, words, string):
        words_lower = [word.lower() for word in words]  # Chuyển các từ cần tìm về chữ thường
        string_lower = string.lower()  # Chuyển chuỗi về chữ thường
        for word in words_lower:
            if word in string_lower:
                return True
            return False
    ###### json
    def remove_prefix_and_suffix(self,string):
        # Tìm vị trí của ký tự '{'
        start_index = string.find('{')
        # Tìm vị trí của ký tự '}'
        end_index = string.rfind('}')
        if start_index != -1 and end_index != -1:
            # Trích xuất phần tử từ vị trí '{' đến vị trí '}'
            result = string[start_index:end_index+1]
            return result
        return None
    def extract_post_infor(self):
        # logger.info("Start")
        json_data = {}
        list_json=[]
        post_infor_element = self.driver.find_elements(By.XPATH,'//script[contains(text(), "responseContext")]')[0]
        post_infor_element_like = self.driver.find_elements(By.XPATH,'//script[contains(text(), "responseContext")]')[1]
        if post_infor_element:
            infor_json = post_infor_element.get_attribute('innerHTML')
            infor_json = self.remove_prefix_and_suffix(infor_json)
            try:
                json_data = json.loads(infor_json)
                jsonpath_expr = parse("$..videoDetails")
                jsonpath_expr2 = parse("$..microformat")
                match = jsonpath_expr.find(json_data)
                match2 = jsonpath_expr2.find(json_data)
                if match and match2:
                    list_json.append(match[0].value)
                    list_json.append(match2[0].value)
            except Exception as e:
                print('Error')
        if post_infor_element_like:
            infor_json_2 = post_infor_element_like.get_attribute('innerHTML')
            infor_json_2 = self.remove_prefix_and_suffix(infor_json_2)
            try:
                json_data_2 = json.loads(infor_json_2)
                jsonpath_expr3 = parse("$..accessibilityText")
                jsonpath_expr4 = parse("$..commentCount")
                match = jsonpath_expr3.find(json_data_2)
                match2 = jsonpath_expr4.find(json_data_2)
                if match2:
                    numbers = re.findall(r'\d+', str(match2[0].value))
                    comment = int(''.join(numbers))
                        
                    list_json.append(comment)
                if match:
                    numbers = re.findall(r'\d+', str(match[0].value))
                    merged_number = int(''.join(numbers))
                        
                    list_json.append(merged_number)
            except Exception as e:
                print(e)
                print('Error')
            

        else:
            print(f" -x-x-x-x-x-x Not found json")
        return list_json

    def extract_video_info_json(self, main_key, sub_key,mode):
        time.sleep(2)
        scroll_height = self.driver.execute_script("return Math.max( document.body.scrollHeight, document.body.offsetHeight, document.documentElement.clientHeight, document.documentElement.scrollHeight, document.documentElement.offsetHeight);")
        self.driver.execute_script(f"window.scrollTo(0, {scroll_height // 16});")
        self.driver.execute_script(f"window.scrollTo(0, {scroll_height // 16});")
        time.sleep(3)
        try:
            
            json_list=self.extract_post_infor()
            data= eval(str(json_list[0]))
            data2= eval(str(json_list[1]))
            self.skip_ads()
            video_info = {}
            current_time = int(datetime.datetime.now().timestamp())
            video_info["time_crawl"] = current_time
            video_info["author"]=data["author"]
            video_info["author_link"]=data2["playerMicroformatRenderer"]["ownerProfileUrl"]
            video_info["link"] = self.driver.current_url    
            video_info["id"] = f'yt_{data["videoId"]}'
            id=data["videoId"]
            video_info["title"] = data["title"]
            video_info["source_id"]=''
            try:
                video_info["description"] = data2["playerMicroformatRenderer"]["description"]["simpleText"]
            except:
                video_info["description"]=''
            text_check=f'{video_info["title"]}\n{video_info["description"]}'
            if main_key !=None and sub_key !=None:
                if self.search_words_in_string(main_key,text_check)==False or self.search_words_in_string(sub_key,text_check)==False:
                    return None,None
            try:
                video_info["hashtag"]=self.extract_hashtags(video_info["description"])
            except:
                video_info["hashtag"]=''
            video_info["type"] = "youtube video"
            video_info["domain"] = "www.youtube.com"
            
            try: 
                video_info["like"] = int(json_list[-1])
            except: 
                video_info["like"]=0
            video_info["avatar"]=self._extract_channel_avatar()
            try:
                video_info["view"]=int(data["viewCount"])
            except:
                video_info["view"]=0
            video_info['created_time'] = int(datetime.datetime.strptime(data2["playerMicroformatRenderer"]["uploadDate"], "%Y-%m-%dT%H:%M:%S%z").timestamp())
            video_info["duration"] = int(data["lengthSeconds"])
            video_info['content']= ''
            time.sleep(2)
            try: 
                video_info["comment"] = int(json_list[-2])
            except: 
                video_info["comment"]=0
            return Post(**video_info)
        except Exception as e:
            print("Exception at extract_video_info_json(): ",e)
            if mode ==1:
                    with open(f'error/error_link.txt', "a") as file:
                        file.write(id)
                        file.write('\n')
            if mode ==2:
                    with open(f'error/{str(video_info["author_link"]).split("/")[-1]}_error.txt', "a") as file:
                        file.write(id)
                        file.write('\n')
            return None

    
    # update 22/09/2023 
    def convert_to_timestamp(self, date_string):
        date_format = "%b/%d/%Y"
        date_obj = datetime.strptime(date_string, date_format)
        timestamp = int(date_obj.timestamp())
        return timestamp
    
    # update 25/09/2023
    def convert_time_to_seconds(self, time_str):
        time_parts = time_str.split(':')
        time_parts = list(map(int, time_parts)) 
        if len(time_parts) == 3:  # Định dạng giờ:phút:giây
            hours, minutes, seconds = time_parts
            total_seconds = hours * 3600 + minutes * 60 + seconds
        elif len(time_parts) == 2:  # Định dạng phút:giây
            minutes, seconds = time_parts
            total_seconds = minutes * 60 + seconds
        else:  # Định dạng giây
            total_seconds = time_parts[0]
        return total_seconds
    
    def comment_validate(self, comment_count):
        try:
            comment_count = comment_count.replace(",", "")
            comment_count = int(comment_count.replace(".", ""))
            return comment_count
        except:
            pass
        return 0

    def extract_like_count(self, like_access_name):
        pattern = r"(\d{1,3}(?:,\d{3})*|\d+)"
        match = re.search(pattern, like_access_name)
        if match:
            number_with_commas = match.group(1)
            return int(number_with_commas.replace(",", ""))
        return None

    def extract_video_id(self, video_link):
        pattern = r"(?:watch\?v=)([a-zA-Z0-9_-]{11})"
        match = re.search(pattern, video_link)
        if match:
            return match.group(1)
        return None

    def preprocess(self, text):
        return re.findall(r"\w+", text.lower())

    def create_index(self, documents):
        index = {}
        for doc_id, text in enumerate(documents):
            for word in self.preprocess(text):
                if word not in index:
                    index[word] = []
                index[word].append(doc_id)
        return index

    def is_exist(self, index, word):
        query_words = self.preprocess(word)
        for word in query_words:
            if word in index:
                return True
        return False

    def check_jamming_video(self, title, description, keyword):
        short_description = self.driver.find_element(
            By.XPATH, '//*[@id="bottom-row"]'
        ).text
        index = self.create_index([short_description, description, title])
        return self.is_exist(index, keyword)
    
    def extract_comments(self, video_id, comment_count):
        self.scroll_down_action(2)
        comment_elements = self.driver.find_elements(By.CSS_SELECTOR, "#body")
        time.sleep(2)

        check_point = 0
        while len(comment_elements) < comment_count and check_point < 3:
            prev_len = len(comment_elements)
            self.scroll_down_action(2)
            time.sleep(5)
            comment_elements = self.driver.find_elements(By.CSS_SELECTOR, "#body")
            if len(comment_elements) == prev_len:
                check_point += 1
            else:
                check_point = 0

        time.sleep(3)
        comments_data = []
        for comment in comment_elements:
            try:
                time_crawl = time.time()
                author_element = comment.find_element(By.ID, "author-text")
                comment_time = comment.find_element(
                    By.CSS_SELECTOR, "#header .published-time-text a"
                ).text
                comment_text = comment.find_element(
                    By.CSS_SELECTOR, "#content-text"
                ).text
                # like_count = comment.find_element(
                #     By.XPATH, '//*[@id="vote-count-middle"]'
                # ).text
                author_id = author_element.get_attribute("href").split("/")[-1]
                # author_name = author_element.text
                author_link = author_element.get_attribute("href")

                raw_comment_data = {
                    'id': f'yt_{author_id}_{video_id}_{time_crawl}',
                    "author": author_id,
                    # "author_name": author_name,
                    # 'like': like_count if like_count != '' else 0,
                    'time_crawl': time_crawl,
                    "author_link": author_link,
                    "created_time": comment_time,
                    "content": comment_text,
                    "source_id": video_id,
                    "type": "comment"
                }

                comments_data.append(
                    Post(**raw_comment_data)
                )
            except Exception as e:
                pass
                # crawler_logger.error(str(e))

        return comments_data

    def _extract_channel_name_and_link(self):
        channel_link = self.driver.find_element(
            By.XPATH, '//span[@itemprop="author"]/link'
        ).get_attribute("href")
        json_text = self.driver.find_element(
            By.XPATH, '//script[@type="application/ld+json"]'
        ).get_attribute("textContent")
        json_data = json.loads(json_text)
        channel_name = json_data["itemListElement"][0]["item"]["name"]
        return channel_link, channel_name
    
    def _extract_channel_avatar(self):
        thumbnail_elements = self.driver.find_elements(By.XPATH, '//*[@id="owner"]/ytd-video-owner-renderer/a/yt-img-shadow/img')
        first_thumbnail_element = thumbnail_elements[0]
        thumbnail_link = first_thumbnail_element.get_attribute("src")
        return str(thumbnail_link)

    def _extract_description(self):
        time.sleep(2)
        short_description = self.driver.find_element(By.XPATH, '//*[@id="expand"]')
        short_description.click()
        time.sleep(2)
        description = self.driver.find_element(
            By.XPATH, '//*[@id="description-inline-expander"]/yt-attributed-string'
        )
        return description.text.strip()
    
    def _extract_hashtag(self):
        try:
            description_element = self.driver.find_element(By.XPATH, '//*[@id="description-inline-expander"]/yt-attributed-string/span')
            span_elements = description_element.find_elements(By.XPATH, './span')
            href_values = []
            for span_element in span_elements:
                a_elements = span_element.find_elements(By.XPATH, './a')
                if not a_elements:
                    continue
                for a_element in a_elements:
                    href = a_element.get_attribute('href')
                    if '/hashtag/' not in href:
                        continue
                    else:
                        hashtag = a_element.text.strip()
                        href_values.append(hashtag)
        except:
            href_values = ''
        return href_values
        #####################

    def detect_views_and_upload_info(self, context):
        patterns = [
            r"([\d,]+)\s+views\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})",
            r"([\d,.]+)\s+lượt xem\s+(\d{1,2})\s+thg\s+(\d{1,2}),\s+(\d{4})",
            r"([\d,]+)\s+views\s+Premiered\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})",
            r"([\d.]+)\s+lượt xem\s+Đã công chiếu vào\s+(\d{1,2})\s+thg\s+(\d{1,2}),\s+(\d{4})",
            r"([\d.]+)\s+lượt xem\s+Đã phát trực tiếp vào\s+(\d{1,2})\s+thg\s+(\d{1,2}),\s+(\d{4})",
            r"([\d]+)\s+views\s+Streamed live on\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})",

            r"([\d,]+)\s+views\s+Premiered"
        ]

        for pattern in patterns:
            match = re.search(pattern, context, re.UNICODE)
            if match:
                try:
                    views = match.group(1).replace(",", "").replace(".", "")
                except:
                    views = 0
                try:
                    day = match.group(2).zfill(2)
                    month = match.group(3).zfill(2)
                    year = match.group(4)
                except:
                    return views, None
                date = f"{day}/{month}/{year}"
            else:
                date= datetime.datetime.now().strftime("%d/%m/%Y")
                return int(views), date
        crawler_logger.error(context)
        return None, None

    def _extract_views_and_upload_info(self):
        short_description = self.driver.find_element(By.XPATH, '//*[@id="bottom-row"]')
        time.sleep(4)
        context = self.driver.find_element(By.XPATH, '//*[@id="info-container"]')
        return self.detect_views_and_upload_info(context.text.strip())

    def extract_hashtags(self, description):
        context = self.driver.find_element(By.XPATH, '//*[@id="info-container"]')
        short_description_hashtags = re.findall(r"#\w+", context.text.strip())
        description_hashtags = re.findall(r"#\w+", description)
        hashtags = list(set(short_description_hashtags) | set(description_hashtags))
        return hashtags

    def skip_ads(self):
        try:
            ads_check = self.driver.find_elements(By.XPATH,
                                                  '//*[contains(@id, "player-overlay:")]')

            if len(ads_check) != 0:
                time.sleep(5)
                skip_ads_button = self.driver.find_element(By.XPATH,
                                                           '//*[contains(@id, "skip-button:")]/span')
                skip_ads_button.click()
        except BaseException as e:
            pass
    def link_to_id(self,link):
        yt = YouTube(link)
        video_id = yt.video_id
        return video_id
    #mode 1: keyword
    #mode 2: channel

    def run(self, video_url, main_key, sub_key,mode):
            print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Bắt đầu crawl link : {video_url}{Colorlog.reset_color}")
            time.sleep(2)
            try:
                self.driver.get(video_url)
                time.sleep(4)
                print(f"{Colorlog.green_color}{datetime.datetime.now()} »» »»»»»» Crawl thông tin video có link là : {video_url}{Colorlog.reset_color}")
                video_info=self.extract_video_info_json(main_key= main_key, sub_key=sub_key,mode=mode)
                if video_info is None:
                    return None,[]
                if int(mode)==3:
                    push_kafka_update(posts=[video_info],comments=None)
                else:
                    push_kafka(posts=[video_info],comments=None)
                comments=[]
                if video_info.comment > 0 :
                    try:
                        comments= self.get_comments_from_url(youtube_url=video_url,mode=mode)
                    except Exception as e:
                        print(e)
                if mode==1:

                    # lưu link vào local
                    with open('link_crawled/crawled.txt', "a") as file:
                        file.write(f'{self.link_to_id(video_url)}\n')

                    # lưu link vào db
                    insert('youtube_video','crawled',self.link_to_id(video_url))

                if mode==2:
                    # lưu link vào local
                    with open(f'link_crawled/{str(video_info.author_link).split("/")[-1]}.txt', "a") as file:
                        file.write(f'{self.link_to_id(video_url)}\n')
                    
                    # lưu link vào db
                    insert('youtube_video',str(video_info.author_link).split("/")[-1],self.link_to_id(video_url))

                self.driver.get('about:blank')
                return video_info, comments
            except Exception as e:
                print(e)
                if mode ==1:
                    with open(f'error/error_link.txt.txt', "a") as file:
                        file.write(self.link_to_id(video_url))
                        file.write('\n')
                if mode ==2:
                    with open(f'error/{str(video_info.author_link).split("/")[-1]}_error.txt', "a") as file:
                        file.write(self.link_to_id(video_url))
                        file.write('\n')
                self.driver.get('about:blank')
                #self.driver.switch_to.window(self.driver.window_handles[1])
                return None,[]
                crawler_logger.error(str(e) + link)
    
    def run_update(self, video_url, main_key, sub_key,mode,check):
            if check==True:
                print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Bắt đầu crawl link : : {video_url}{Colorlog.reset_color}")
                time.sleep(2)
                try:
                    self.driver.get(video_url)
                    time.sleep(4)
                    print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Crawl thông tin video có link là: {video_url}{Colorlog.reset_color}")
                    video_info=self.extract_video_info_json(main_key= main_key, sub_key=sub_key,mode=mode)
                    if video_info is None:
                        return None,[]
                    if int(mode)==3:
                        push_kafka_update(posts=[video_info],comments=None)
                    else:
                        push_kafka(posts=[video_info],comments=None)
                    comments=[]
                    if video_info.comment > 0 :
                        try:
                            comments= self.get_comments_from_url(youtube_url=video_url,mode=mode)
                        except Exception as e:
                            print(e)
                    if mode==1:

                        # lưu link vào local
                        with open('link_crawled/crawled.txt', "a") as file:
                            file.write(f'{self.link_to_id(video_url)}\n')

                        # lưu link vào db
                        #insert('youtube_video','crawled',self.link_to_id(video_url))

                    if mode==2:
                        # lưu link vào local
                        with open(f'link_crawled/{str(video_info.author_link).split("/")[-1]}.txt', "a") as file:
                            file.write(f'{self.link_to_id(video_url)}\n')
                        
                        # lưu link vào db
                        #insert('youtube_video',str(video_info.author_link).split("/")[-1],self.link_to_id(video_url))

                    self.driver.get('about:blank')
                    return video_info, comments
                except Exception as e:
                    print(e)
                    if mode ==1:
                        with open(f'error/error_link.txt.txt', "a") as file:
                            file.write(self.link_to_id(video_url))
                            file.write('\n')
                    if mode ==2:
                        with open(f'error/{str(video_info.author_link).split("/")[-1]}_error.txt', "a") as file:
                            file.write(self.link_to_id(video_url))
                            file.write('\n')
                    self.driver.get('about:blank')
                    #self.driver.switch_to.window(self.driver.window_handles[1])
                    return None,[]
            else:
                self.driver.get('about:blank')

    def ajax_request(self, endpoint, ytcfg, retries=5, sleep=20):
        url = 'https://www.youtube.com' + endpoint['commandMetadata']['webCommandMetadata']['apiUrl']
        data = {'context': ytcfg['INNERTUBE_CONTEXT'],
                'continuation': endpoint['continuationCommand']['token']}
        for _ in range(retries):
            response = self.session.post(url, params={'key': ytcfg['INNERTUBE_API_KEY']}, json=data)
            if response.status_code == 200:
                return response.json()
            if response.status_code in [403, 413]:
                return {}
            else:
                time.sleep(sleep)

# Update 26/09/2023:
# - Crawl các thông tin của comment
    def scroll_down(self):
        while True:
            before_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(5)
            after_scroll_height = self.driver.execute_script("return document.documentElement.scrollHeight")

            if after_scroll_height == before_scroll_height:
                break
    def get_comments_from_url(self,youtube_url,mode):
        print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Crawl bình luận của video: {youtube_url}{Colorlog.reset_color}")
        link = self.driver.current_url
        video_id = self.extract_video_id(link)
        sort_by=SORT_BY_RECENT 
        language=None
        sleep=.4
        self.scroll_down()
        time.sleep(2)

        parsed_url = urlparse(youtube_url)
        query_params = parse_qs(parsed_url.query)
        video_id = query_params['v'][0]
        response = self.session.get(youtube_url)
        self.scroll_down()
        if 'consent' in str(response.url):
            params = dict(re.findall(YT_HIDDEN_INPUT_RE, response.text))
            params.update({'continue': youtube_url, 'set_eom': False, 'set_ytc': True, 'set_apyt': True})
            response = self.session.post('https://consent.youtube.com/save', params=params)

        html = response.text
        ytcfg = json.loads(self.regex_search(html, YT_CFG_RE, default=''))
        if not ytcfg:
            return  
        if language:
            ytcfg['INNERTUBE_CONTEXT']['client']['hl'] = language

        data = json.loads(self.regex_search(html, YT_INITIAL_DATA_RE, default=''))

        item_section = next(self.search_dict(data, 'itemSectionRenderer'), None)
        renderer = next(self.search_dict(item_section, 'continuationItemRenderer'), None) if item_section else None
        if not renderer:
            return

        sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu:
            section_list = next(self.search_dict(data, 'sectionListRenderer'), {})
            continuations = list(self.search_dict(section_list, 'continuationEndpoint'))
            data = self.ajax_request(continuations[0], ytcfg) if continuations else {}
            sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu or sort_by >= len(sort_menu):
            raise RuntimeError('Failed to set sorting')
        continuations = [sort_menu[sort_by]['serviceEndpoint']]

        while continuations:
            continuation = continuations.pop()
            response = self.ajax_request(continuation, ytcfg)

            if not response:
                break

            error = next(self.search_dict(response, 'externalErrorMessage'), None)
            if error:
                raise RuntimeError('Error returned from server: ' + error)

            actions = list(self.search_dict(response, 'reloadContinuationItemsCommand')) + \
                      list(self.search_dict(response, 'appendContinuationItemsAction'))
            for action in actions:
                for item in action.get('continuationItems', []):
                    if action['targetId'] in ['comments-section',
                                              'engagement-panel-comments-section',
                                              'shorts-engagement-panel-comments-section']:
                        continuations[:0] = [ep for ep in self.search_dict(item, 'continuationEndpoint')]
                    if action['targetId'].startswith('comment-replies-item') and 'continuationItemRenderer' in item:
                        continuations.append(next(self.search_dict(item, 'buttonRenderer'))['command'])

            comments_data = []
            for comment in reversed(list(self.search_dict(response, 'commentRenderer'))):
                time_crawl= int(datetime.datetime.now().timestamp())
                result = {
                    'time_crawl': time_crawl,
                    'id': 'yt_'+str(comment['commentId']),
                    'content': str(''.join([c['text'] for c in comment['contentText'].get('runs', [])])),
                    'time': comment['publishedTimeText']['runs'][0]['text'],
                    'author': str(comment.get('authorText', {}).get('simpleText', '')),
                    'author_link': str('https://www.youtube.com/channel/'+comment['authorEndpoint']['browseEndpoint'].get('browseId', '')),
                    'avatar': str(comment['authorThumbnail']['thumbnails'][-1]['url']),
                    'like': int(comment.get('voteCount', {}).get('simpleText', '0')),
                    # 'heart': next(self.search_dict(comment, 'isHearted'), False),
                    'reply': int(comment.get('replyCount', 0)),
                    'domain': 'www.youtube.com',
                    'title': '',
                    'duration': 0,
                    'view': 0,
                    'type': 'youtube comment',
                    'source_id': 'yt_'+str(video_id),
                    'decription': '',
                    'link': str(youtube_url+"&lc="+comment['commentId'])
                        }
                try:
                    result['created_time'] = int(dateparser.parse(result['time'].split('(')[0].strip()).timestamp())
                except AttributeError:
                    pass    

                yield result
                comments_data.append(result)
            if len(comments_data)>0:
                print(f"{Colorlog.green_color}{datetime.datetime.now()} »» ⨝ Đã crawl được {len(comments_data)} bình luận{Colorlog.reset_color}")
                for comment in comments_data:
                    if int(mode)==3:
                        push_kafka_update(posts=[Post(**comment)],comments=None)
                    else:
                        push_kafka(posts=[Post(**comment)],comments=None)
            time.sleep(sleep)
        return comments_data

    @staticmethod
    def regex_search(text, pattern, group=1, default=None):
        match = re.search(pattern, text)
        return match.group(group) if match else default

    @staticmethod
    def search_dict(partial, search_key):
        stack = [partial]
        while stack:
            current_item = stack.pop()
            if isinstance(current_item, dict):
                for key, value in current_item.items():
                    if key == search_key:
                        yield value
                    else:
                        stack.append(value)
            elif isinstance(current_item, list):
                stack.extend(current_item)

class YoutubeUtil:
    def __init__(self, driver):
        self.driver = driver

    def skip_ads(self):  # Loading
        try:
            skip_ad = self.driver.find_element(
                By.XPATH, '//*[@id="skip-button:n"]/span/button'
            )
            skip_ad.click()
            time.sleep(5)
        except Exception as e:
            time.sleep(1)

    def subscribe_channel(self):
        subcribe_button = self.driver.find_element(
            By.XPATH, "//*[@id='subscribe-button-shape']/button"
        )
        if subcribe_button.text:
            subcribe_button.click()
            crawler_logger.info(f"Subcribe to channel: {self.driver.current_url}")
        else:
            crawler_logger.info(f"Subcribed before: {self.driver.current_url}")

    def like_video(self):
        like_button = self.driver.find_element(
            By.XPATH,
            "//*[@id='segmented-like-button']/ytd-toggle-button-renderer/yt-button-shape/button",
        )
        isLike = like_button.get_attribute("aria-pressed")
        if isLike == "false":
            crawler_logger.info(f"Like: {self.driver.current_url}")
            like_button.click()
        else:
            crawler_logger.info(f"Liked berfore: {self.driver.current_url}")

    def comment(self, text):
        try:
            comment_box = self.driver.find_element(
                By.XPATH, "//div[@id='placeholder-area']"
            )
            comment_box.click()
            time.sleep(2)
            comment_input = self.driver.find_element(
                By.XPATH, '//*[@id="contenteditable-root"]'
            )
            comment_input.send_keys(text)
            time.sleep(2)

            submit_button = self.driver.find_element(
                By.XPATH, '//*[@id="submit-button"]/yt-button-shape/button'
            )
            submit_button.click()
            crawler_logger.info(f"Commented at {self.driver.current_url}")
            time.sleep(3)
        except Exception as e:
            crawler_logger.error(
                f"Comment error: {str(e)} at {self.driver.current_url}"
            )
            pass

    def report(self):
        option_button = self.driver.find_element(
            By.XPATH, '//*[@id="button-shape"]/button/yt-touch-feedback-shape/div'
        )
        option_button.click()
        time.sleep(4)

        addtional_elemets = self.driver.find_elements(
            By.XPATH, '//*[@id="items"]/ytd-menu-service-item-renderer[3]'
        )

        for element in addtional_elemets:
            if element.text == "Report":
                element.click()
        time.sleep(3)
        reason_checkbox = self.driver.find_element(
            By.XPATH,
            '//*[@id="yt-options-renderer-options"]/tp-yt-paper-radio-button[2]',
        )
        reason_checkbox.click()
        time.sleep(4)
        reason_dropdown = self.driver.find_element(
            By.XPATH,
            '//*[@id="yt-options-renderer-options"]/tp-yt-paper-dropdown-menu[2]',
        )
        reason_dropdown.click()

        desired_option_text = "Adults fighting"
        # desired_option_text = 'Cảnh tấn công vào cơ thể'
        options = self.driver.find_elements(By.CSS_SELECTOR, "tp-yt-paper-item")

        for option in options:
            if option.text.strip() == desired_option_text:
                option.click()
                break

        time.sleep(4)
        submit_button = self.driver.find_element(By.XPATH, '//*[@id="submit-button"]')
        submit_button.click()
        time.sleep(3)
        confirm_report_button = self.driver.find_element(
            By.XPATH, '//*[@id="submit-button"]/yt-button-renderer/yt-button-shape/button'
        )
        confirm_report_button.click()
        time.sleep(2)
        close_noti_button = self.driver.find_element(
            By.XPATH, '//*[@id="confirm-button"]/yt-button-shape/button'
        )
        close_noti_button.click()
        time.sleep(2)

    def handle_interact_option(self, interact_option: InteractOption):
        if interact_option.like_mode:
            self.like_video()
            time.sleep(2)
        if interact_option.report_mode:
            try:
                self.report()
                crawler_logger.info("Successfully Reported")
            except Exception as e:
                crawler_logger.error("Fail to report")
            time.sleep(2)

    def video_click(self):
        try:
            filter_xpath = '//*[@id="tabsContent"]/tp-yt-paper-tab[2]'
            video_filter = self.driver.find_element(By.XPATH, filter_xpath)
            video_filter.click()
            time.sleep(2)
        except BaseException as e:
            pass
    
    def recently_click(self):
        try:
            filter_xpath = '//*[@id="chips"]/yt-chip-cloud-chip-renderer[6]'
            recently_button = self.driver.find_element(By.XPATH, filter_xpath)
            recently_button.click()
            time.sleep(3)
        except Exception as e:
            pass

class StringHandler:

    @staticmethod
    def extract_hashtag_views(input_string):
        pattern = r'(\d+\.\d+|\d+)([KMB])?\s*videos'
        match = re.search(pattern, input_string)

        if match:
            # Extract the matched value and unit
            matched_value = float(match.group(1))
            unit = match.group(2)

            # Convert the value based on the unit if it exists
            if unit == 'K':
                approximate_videos = matched_value * 1000
            elif unit == 'M':
                approximate_videos = matched_value * 10 ** 6  # 'M' represents millions
            elif unit == 'B':
                approximate_videos = matched_value * 10 ** 9
            else:
                approximate_videos = matched_value
            return approximate_videos
        return None


class GChromeDriver:

    @classmethod
    def init_driver(cls, options_list,username=None, password=None):
        local_device_config = device_config_ultils.get_local_device_config()
        useChromium=local_device_config[0]['useChromium']
        service_path=Service(local_device_config[0]['paramaterChromium']['service_path'])
        binary_location=local_device_config[0]['paramaterChromium']['binary_location']
        proxy=local_device_config[0]['account']['proxy']
        options = Options()

        if proxy !='null' and proxy!=None and proxy!="":
            options.add_argument(f'--proxy-server={proxy}')
        options.add_experimental_option("detach", False)
        if useChromium==1:
            options_list= local_device_config[0]['listArgumentChromium']
            options.binary_location=binary_location
            service=service_path
        else:
            service=None
        for option in options_list:
            options.add_argument(str(option))
        try:
            driver = webdriver.Chrome(service=service,options=options,executable_path='chromedriver_c.exe')
        except:
            driver = webdriver.Chrome(service=service,options=options)
        driver.maximize_window()
        if username and password:
            GChromeDriver.login(driver, username, password)
        return driver

    @classmethod
    def login(cls, driver, username, password):
        driver.get(LOGIN_URL)
        username_field = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="identifierId"]'))
        )
        username_field.send_keys(username)
        username_next_button = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="identifierNext"]/div/button'))
        )
        username_next_button.click()
        time.sleep(5)
        GChromeDriver.captcha_handle(driver)
        
        password_field = None
        max_attempts = 3
        attempts = 0
        while attempts < max_attempts:
            try:
                password_field = WebDriverWait(driver, 40).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="password"]/div[1]/div/div[1]/input'))
                )
                break  # Thoát khỏi vòng lặp nếu tìm thấy trường mật khẩu
            except TimeoutException:
                attempts += 1
                print(f"{Colorlog.red_color}{datetime.datetime.now()} Không tìm thấy trường mật khẩu. Refresh lại trang...{Colorlog.reset_color}")
                driver.refresh()
        
        if password_field:
            time.sleep(5)
            password_field.send_keys(password)
            password_next_button = WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="passwordNext"]/div/button'))
            )
            password_next_button.click()
            time.sleep(5)
        else:
            print(f"{Colorlog.red_color}{datetime.datetime.now()} Cố gắng đăng nhập thất bại{Colorlog.reset_color}")

    @classmethod
    def captcha_handle(cls, driver):
        try:
            driver.find_element(By.XPATH, '//*[@id="i1"]').click()
        except BaseException as e:
            pass
