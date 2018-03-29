import os
import re
import csv
from queue import Queue

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

csv_files = [unzipped_files + '/' + file for file in os.listdir(unzipped_files)]
pattern = re.compile(r'v1:\d+:\d+:\d+:\w+-\w+-\w+-\w+-\w+\Z')
data = []

for file in csv_files:
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if pattern.match(row['user:scalr-meta']):
                data.append({
                    'user:scalr-meta': row['user:scalr-meta'],
                    'Cost': row['Cost']
                })
