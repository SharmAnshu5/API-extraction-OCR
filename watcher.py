from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from main import process_file


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            process_file(event.src_path)


def start_watch(folder):
    observer = Observer()
    observer.schedule(Handler(), folder, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
