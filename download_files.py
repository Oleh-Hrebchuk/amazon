import urllib.request
import threading


class DownloadFiles(threading.Thread):
    def __init__(self, queue, year, download_folder):
        threading.Thread.__init__(self)
        self.queue = queue
        self.year = year
        self.download_folder = download_folder

    def run(self):
        while True:
            day = self.queue.get()

            self.download_file(day)

            self.queue.task_done()

    def download_file(self, day):
        url = 'https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-{}-{}.csv.zip'.format(self.year, day)  # noqa
        urllib.request.urlretrieve(
            url, '{}{}-{}.zip'.format(self.download_folder, self.year, day)
        )
