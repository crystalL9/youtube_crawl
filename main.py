from auto import JsonProcessor
import threading

if __name__ == "__main__":
    filename = "dev_config.json"
    processor = JsonProcessor(filename)
    watch_thread = threading.Thread(target=processor.watch_file_changes)
    watch_thread.start()
    watch_thread.join()
