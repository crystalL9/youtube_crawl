import requests
import logging
from colorlog import ColoredFormatter
from dotenv import load_dotenv
import os

load_dotenv()


# Tạo logger và cấu hình logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Tạo một StreamHandler để đẩy log message đến stdout
console_handler = logging.StreamHandler()

# Sử dụng ColoredFormatter để có log màu trên màn hình
formatter = ColoredFormatter(
    "%(log_color)s%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    reset=True,
    log_colors={
        'DEBUG': 'white',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'white',
    },
    secondary_log_colors={},
    style='%'
)

console_handler.setFormatter(formatter)

# Thêm StreamHandler vào logger
logger.addHandler(console_handler)

# Địa chỉ API
api_address= os.getenv("api_address")

# API lấy các link đã crawl của 1 id
def get_links(table_name, object_id):
    url = f"{api_address}/get-links/{table_name}/{object_id}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error: {str(e)}")

# API insert link đã crawl vào db
def insert(table_name, object_id, links):
    if isinstance(links, list):
        links = ",".join(links)
    url = f"{api_address}/insert/{table_name}/{object_id}?new_links={links}"
    try:
        response = requests.post(url)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Insert thành công {links}và {table_name}.{object_id}")
            return result
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error: {str(e)}")
