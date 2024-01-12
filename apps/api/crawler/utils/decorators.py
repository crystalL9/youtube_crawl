import time

from apps.api.crawler.utils.crawler_logger import CrawlerLogger

crawler_logger = CrawlerLogger()


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        end_time = time.time()
        elapsed_time = end_time - start_time
        crawler_logger.info(
            f"Function '{func.__name__}' took {elapsed_time:.4f} seconds to run."
        )
        return func(*args, **kwargs)

    return wrapper
