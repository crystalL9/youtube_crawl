import inspect
import logging


class CrawlerLogger:
    def __init__(self):
        self.info_logger = self.setup_logger("info_logger", logging.INFO)
        self.error_logger = self.setup_logger(
            "error_logger", logging.ERROR
        )

    def setup_logger(self, logger_name, log_level):
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(log_level)
        stream_handler.setFormatter(formatter)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger.addHandler(stream_handler)

        return logger

    def info(self, message):
        self.info_logger.info(message)

    def error(self, message):
        caller_class = inspect.stack()[1][0].f_locals.get("self", None)
        if caller_class:
            class_name = caller_class.__class__.__name__
            error_method = inspect.stack()[1][3]
            message = f"{class_name}: {error_method} - {message}"
        self.error_logger.error(message)

    def close(self):
        for handler in self.info_logger.handlers:
            handler.close()
        for handler in self.error_logger.handlers:
            handler.close()
