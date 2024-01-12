"""
    AI se cu ri ty wi th lo ve
"""
class Post:
    def __init__(self, **kwargs):
        self.id = kwargs.get('id', '')
        self.type = kwargs.get('type', '')
        self.time_crawl = kwargs.get('time_crawl', '')
        self.link = kwargs.get('link', '')
        self.author = kwargs.get('author', '')
        self.author_link = kwargs.get('author_link', '')
        self.avatar = kwargs.get('avatar', '')
        self.created_time = kwargs.get('created_time', '')
        self.content = kwargs.get('content', '')
        self.like = kwargs.get('like', 0)
        self.comment = kwargs.get('comment', 0)
        self.domain = kwargs.get('domain', 'youtube.com')
        self.hashtag =  kwargs.get('hashtag', [])
        self.title = kwargs.get('title', '')
        self.duration = kwargs.get('duration', 0 )
        self.view = kwargs.get('view', 0)
        self.description = kwargs.get('description', '')
        self.source_id = kwargs.get('source_id', '')
            
    #Hàm xác định xem post được crawl đủ hay chưa
    def is_valid(self) -> bool:
        is_valid = self.id != "" and self.author != "" and self.link != "" and self.created_time 
        return is_valid

    def __str__(self) -> str:
        string = ""
        for attr_name, attr_value in self.__dict__.items():
            string =  f"{attr_name}={attr_value}\n" + string
        return string