import json

# Đường dẫn đến tệp a.txt
file_path = "keywordosint.txt"

# Đọc nội dung tệp a.txt
with open(file_path, 'r', encoding='utf-8') as file:
    text_content = file.read()

# Tạo biến JSON với key chứa giá trị văn bản
json_data = {
    'key': text_content
}

# Đường dẫn đến tệp JSON đầu ra
output_file_path = 'output.json'

# Ghi dữ liệu JSON vào tệp JSON đầu ra
with open(output_file_path, 'a', encoding='utf-8') as output_file:
    json.dump(json_data, output_file)

# In thông báo khi hoàn thành
print("Đã tạo tệp JSON thành công.")