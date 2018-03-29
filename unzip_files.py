import threading
import zipfile


class UnzipFiles(threading.Thread):
    def __init__(self, queue, year, download_folder, unzipped_files):
        threading.Thread.__init__(self)
        self.queue = queue
        self.year = year
        self.download_folder = download_folder
        self.unzipped_files = unzipped_files

    def run(self):
        while True:
            day = self.queue.get()

            self.download_file(day)

            self.queue.task_done()

    def download_file(self, day):
        with zipfile.ZipFile('{}{}-{}.zip'.format(self.download_folder, self.year, day), "r") as zip_ref:   # noqa
            zip_ref.extractall(self.unzipped_files)
