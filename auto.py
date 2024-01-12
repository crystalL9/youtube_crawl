import json
import threading
import time
from queue import Queue
import work
import ctypes
from logger import Colorlog
class JsonProcessor:
    def __init__(self, filename):
        self.filename = filename
        self.processed_elements = set()
        self.queue = Queue()
        self.lock = threading.Lock()
        self.running_threads = {}
        self.array_thread=[]
    def watch_file_changes(self):
        while True:
            try:
                with open(self.filename, 'r') as file:
                    data = json.load(file)
                    existing_elements = list(self.running_threads.keys())
                    for config_element in data:
                        element_id = config_element.get('id')
                        if element_id and element_id not in self.get_processed_elements():
                            self.add_processed_element(element_id)
                            self.queue.put(config_element.copy())
                            if element_id not in existing_elements:
                                process_thread = threading.Thread(target=self.process_config_element)
                                self.running_threads[element_id] = process_thread
                                _thread={}
                                _thread['id']=element_id
                                _thread['status']=1
                                self.array_thread.append(_thread)
                                existing_elements.append(element_id)
                                print(f"{Colorlog.purple_color}Running thread PID: {process_thread.ident}, Config element: {config_element.get('id')}{Colorlog.reset_color}")
                                process_thread.start()
                    deleted_elements = set(existing_elements) - set([config_element.get('id') for config_element in data])
                    if deleted_elements:
                        print(f"{Colorlog.cyan_color}Config{deleted_elements} has just been deleted, wait for 1 round to stop completely{Colorlog.reset_color}" )
                        for deleted_element in deleted_elements:
                            id=deleted_element
                            self.set_thread(id)
                            self.stop_thread(deleted_element)
            except Exception as e:
                print(e)
                pass
            finally:
                time.sleep(3)
    def stop_thread(self, element_id):
        thread_info = self.running_threads.get(element_id)
        if thread_info:
            thread = thread_info
            thread_id = thread.ident
            thread_info_array = next((t for t in self.array_thread if t['id'] == element_id), None)
            if thread_info_array:
                thread_info_array['status'] = 0 

            thread.join(timeout=1)
            if thread.is_alive():
                self.terminate_thread(thread_id)
            del self.running_threads[element_id]
    def is_alive(self, thread_id):
        thread = self.running_threads.get(thread_id)
        return thread and thread.is_alive()

    def terminate_thread(self, thread_id):
        thread = self.running_threads.get(thread_id)
        if thread:
            thread.join(timeout=1)
            if thread.is_alive():
                if hasattr(thread, '_stop'):
                    thread._stop()
                else:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id), ctypes.py_object(SystemExit))
                self.running_threads.pop(thread_id, None)

    def get_thread(self,id):
        for thread in self.array_thread:
            if thread['id']==id:
                return thread['status']
            
    def set_thread(self,id):
        for thread in self.array_thread:
            if thread['id']==id:
                thread['status']=0
            
    def remove_thread(self,id):
        for thread in self.array_thread:
            if thread['id']==id:
                self.array_thread.remove(thread)
                
    def process_config_element(self):
        config_element = self.queue.get()
        local_device_config = [config_element]
        id = local_device_config[0]['id']
        mode_id = local_device_config[0]['mode']['id']
        time_update=local_device_config[0]['mode']['start_time_run']
        while True:
            status=self.get_thread(id)
            if status==0:
                self.remove_thread(id)     
                break
            if mode_id == 3 :
                work.update(local_device_config=local_device_config)
                time.sleep(1*60)
            else:
                try:
                        try:
                            if local_device_config[0]['mode']['id'] == 1:
                                work.work_keyword(local_device_config=local_device_config)
                            elif local_device_config[0]['mode']['id'] == 2:
                                work.work1(local_device_config=local_device_config)
                            else:
                                print("Valid Config")
                        except Exception as e:
                            print(f"Error processing config: {e}")
                        print("Waiting 30 minutes to new loop")
                        time.sleep(30*60)
                    
                except Exception as e:
                    print(e)
                    continue

    def add_processed_element(self, element_id):
        with self.lock:
            self.processed_elements.add(element_id)

    def get_processed_elements(self):
        with self.lock:
            return self.processed_elements


