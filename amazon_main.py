import os
import re
import csv
from queue import Queue
import threading
import datetime
from download_files import DownloadFiles
from unzip_files import UnzipFiles

year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'downloads/'
unzipped_files = 'unzipped_files'


# download
queue = Queue()
for i in range(len(dates)):
    d = DownloadFiles(queue, year, download_folder)
    d.setDaemon(True)
    d.start()

for url in dates:
    queue.put(url)

queue.join()

# unzip
queue_unzip = Queue()
for i in range(len(dates)):
    d = UnzipFiles(queue_unzip, year, download_folder, unzipped_files)
    d.setDaemon(True)
    d.start()

for url in dates:
    queue_unzip.put(url)

queue_unzip.join()
print(datetime.datetime.now())
csv_files = [unzipped_files + '/' + file for file in os.listdir(unzipped_files)]
# pattern = re.compile(r'v1:\d+:\d+:\d+:\w+-\w+-\w+-\w+-\w+\Z')
# data = []
# sclar2 = {}
# # TODO transofrm to threads
# for file in csv_files:
#     with open(file) as csvfile:
#         reader = csv.DictReader(csvfile)
#
#         for row in reader:
#             user = row['user:scalr-meta']
#             cost = row['Cost']
#             if pattern.match(user):
#                 if user not in sclar2:
#                     sclar2[user] = [float(cost)]
#                 else:
#                     sclar2[user].append(float(cost))


sclar = {}


class Cost(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self._pattern = re.compile(r'v1:\d+:\d+:\d+:\w+-\w+-\w+-\w+-\w+\Z')

    def run(self):
        while True:
            file = self.queue.get()

            self.cost(file)

            self.queue.task_done()

    def cost(self, file):
        with open(file) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                user = row['user:scalr-meta']
                cost = row['Cost']
                if self._pattern.match(user):
                    if user not in sclar:
                        sclar[user] = [float(cost)]
                    else:
                        sclar[user].append(float(cost))


# calc
queue_caclc = Queue()
for i in range(len(dates)):
    d = Cost(queue_caclc)
    d.setDaemon(True)
    d.start()

for file in csv_files:
    queue_caclc.put(file)

queue_caclc.join()
print(datetime.datetime.now())
