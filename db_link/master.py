import os
import pymongo

# Đọc từ file và thêm vào db
def insert_from_file_to_db(folder_path, table_name, db_name, db_address):
    array_link=[]
    file_list = os.listdir(folder_path)

    for file_name in file_list:
        try:
            file_path = os.path.join(folder_path, file_name)

            if os.path.isfile(file_path):
                with open(file_path, 'r', encoding='utf-8') as file:
                    print(f"File: {file_name}")
                    for line in file:
                        array_link.append(line.strip())
                    print('=' * 30)
                new_document = {"object": f"{(file_name.split('.txt'))[0]}",
                                "link":array_link}
                client = pymongo.MongoClient(f"{db_address}") 
                db = client[f"{db_name}"]
                col = db[f"{table_name}"]
                result = col.insert_one(new_document)
                print(f"❇️ Insert to {db_name}.{table_name} successfully")
                client.close()

        except Exception as e:
            print("An error occurred. Detail: ",e)
            pass
    
# thêm 1 docment mới vào db
def insert_ids(id,list_link, table_name, db_name, db_address):
    try:
        client = pymongo.MongoClient(f"{db_address}") 
        db = client[f"{db_name}"]
        col = db[f"{table_name}"]
        new_document = {"object": f"{id}",
                        "link":list_link}
        result = col.insert_one(new_document)
        print(f"❇️ Insert to {db_name}.{table_name} successfully")

        client.close()
    except Exception as e:
        print("An error occurred. Detail: ",e)
    
# Tạo document (nếu chưa tồn tại), update document (nếu đã tồn tại) 
def insert_or_update_ids(object, new_link, table_name, db_name, db_address):
    try:
        client = pymongo.MongoClient(f"{db_address}")
        db = client[f"{db_name}"] 
        col = db[f"{table_name}"] 
        existing_document = col.find_one({"object": f"{object}"})
        if existing_document:
            col.update_one(
                {"object": f"{object}"},
                {"$addToSet": {"link": new_link}}
            )
            print(f"✅ Link added to existing document in {db_name}.{table_name} successfully")
        else:
            new_document = {
                "object": f"{object}",
                "link": [new_link]
            }
            col.insert_one(new_document)
            print(f"❇️ New document created in {db_name}.{table_name} successfully")
        client.close()
    except Exception as e:
        print("An error occurred. Detail: ", e)

# Lấy data từ db
def get_ids(table_name, db_name, db_address):
    client = pymongo.MongoClient(f"{db_address}") 
    db = client[f"{db_name}"]
    col = db[f"{table_name}"]
    x = col.find()
    for y in x:
        print(y)

# xóa toàn bộ các document trong bảng
def drop_all_documents(db_name, table_name, db_address):
    try:
        client = pymongo.MongoClient(f"{db_address}")
        db = client[f"{db_name}"]
        col = db[f"{table_name}"]
        col.delete_many({})
        print(f"❌ All documents in {db_name}.{table_name} deleted successfully")
        client.close()
    except Exception as e:
        print(f"❌ Error while deleting documents: {e}")

# xóa bảng chỉ định
def drop_table(db_name, table_name, db_address):
    try:
        client = pymongo.MongoClient(f"{db_address}")
        db = client[f"{db_name}"]
        col = db[f"{table_name}"]
        col.drop()
        print(f"❌ Table {table_name} in {db_name} dropped successfully")
        client.close()
    except Exception as e:
        print(f"❌ Error while dropping table: {e}")

# xóa toàn bộ các bảng
def drop_all_tables(db_name, db_address):
    try:
        client = pymongo.MongoClient(f"{db_address}")
        db = client[f"{db_name}"]
        collections = db.list_collection_names()
        for collection_name in collections:
            col = db[collection_name]
            col.drop()
            print(f"❌ Table {collection_name} in {db_name} dropped successfully")
        client.close()

    except Exception as e:
        print(f"❌ Error while dropping tables: {e}")

if __name__ == "__main__":

    db_address = 'mongodb://192.168.143.92:27017/'
    db_name = 'OSINT_db'
    table_name = 'youtube_video'
    folder_path="test"
    #insert_from_file_to_db(folder_path, table_name, db_name, db_address)
    #insert_ids(list_ids, table_name, db_name, db_address)
    #drop_all_documents(db_name, table_name, db_address)
    #insert_or_update_ids('test', ['123','234'], table_name, db_name, db_address)
    print("Data in Databse :")
    get_ids(table_name, db_name, db_address)
