import pickle

from kafka import KafkaProducer
from logger import Colorlog
import datetime
from dotenv import load_dotenv
import os

load_dotenv()
kafka_address = os.getenv("KAFKA_address")
topic_raw = os.getenv("KAFKA_topic")
topic_update = os.getenv("KAFKA_topic_udpate")

producer = KafkaProducer(bootstrap_servers=[f"{kafka_address}"])



def push_kafka(posts, comments):
    if posts:
        bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
        producer.send(f'{topic_raw}', bytes_obj)
        print(f"{Colorlog.yellow_color}{datetime.datetime.now()} »» Đã 1 object đẩy vào kafka {Colorlog.reset_color}")
        return 1
    else:
        return 0
def push_kafka_update(posts, comments):
    if posts:
        bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
        producer.send(f'{topic_update}', bytes_obj)
        print(f"{Colorlog.yellow_color}{datetime.datetime.now()} »» Đã 1 object update đẩy vào kafka {Colorlog.reset_color}")
        return 1
    else:
        return 0


class GeneratorPost:
    def __init__(self, target, args: list = []) -> None:
        self.target = target
        self.args = args

    def run(self):
        for posts in self.target(*self.args):
            print(f"số bài post group đẩy qua kafka là {len(posts)}")
            push_kafka(posts=posts)
            # for post in posts:
            #     write_log_post(post)
            # if self.is_return:
            #     return post

    def get_posts(self, list_posts: list):
        for posts in self.target(*self.args):
            print(f"số bài posts là {len(posts)}")
            list_posts.extend(posts)
            push_kafka(posts=posts)
            # for post in posts:
            #     write_log_post(post)


# ## Hàm test kết quả đẩy qua kafka => trả về list các object Post
# def Test(paramater):
#     post = Post
#     for i in range(100):
#         print(i)
#         yield [post]


# ##Demo sử dụng
# if __name__ == "__main__":
#     post = {
#         'source_id': 'yt_yr2HZQdXeG4ad',
#         'description': '',
#         'view': 0,
#         'duration': 0,
#         'title': '',
#         'hastag': [],
#         'domain': 'www.youtube.com',
#         'comment': 0,
#         'like': 1,
#         'content': 'ông hô chi minh chết cũng dối trá ngày chết là hiểu rồi,he,he',
#         'created_time': 1475482572.844257,
#         'avatar': 'https://yt3.ggpht.com/ytc/APkrFKZpRXZlv7xkz4c-t8kbB3zqYKABVBfjXhsp_Q=s176-c-k-c0x00ffffff-no-rj',
#         'author_link': 'https://www.youtube.com/channel/UCeFzH2oO4aPJXmoSrD6tqjA',
#         'author': '@minhthuy655',
#         'link': 'https://www.youtube.com/watch?v=yr2HZQdXeG4&lc=Ugjs11krTAZyI3gCoAEC',
#         'time_crawl': 1696320973,
#         'type': 'youtube comment',
#         'id': 'yt_Ugjs11krTAZyI3gCoAEC_test'
#     }
#     print(post)
#     # Đẩy 1 lần
#     # post = Post
#     push_kafka(posts=post,comments=None)
