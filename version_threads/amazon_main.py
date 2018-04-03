import os
import re
import csv
import threading
import sqlite3

from queue import Queue
from collections import defaultdict

from download_files import DownloadFiles
from unzip_files import UnzipFiles
import sqlite_manager


year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'downloads/'
unzipped_files = 'unzipped_files'
sclar = defaultdict(list)
pattern = re.compile(r'v1:\d+:\d+:\d+:\w+-\w+-\w+-\w+-\w+\Z')
csv_files = [unzipped_files + '/' + file for file in os.listdir(unzipped_files)]

conn = sqlite3.connect('amazon.sqlite3')
cursor = conn.cursor()


def write_cost_data(data, slice='v1:'):
    for key, val in data.items():
        db_val = cursor.execute(
            "SELECT * FROM cost WHERE object_type = '{}'".format(
                key)).fetchone()

        if db_val:

            try:
                suma = sum(val) + float(db_val[2])
            except TypeError:
                continue
            cursor.execute(
                "UPDATE cost SET cost={} WHERE object_type = '{}'".format(suma, key))
            conn.commit()
        else:

            suma = sum(val)
            cursor.execute(
                "INSERT INTO cost VALUES ('{}', '{}', {})".format(
                    key, key[len(slice):], suma))
            conn.commit()


def cost(file):
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            user = row['user:scalr-meta']
            cost = row['Cost']
            if pattern.match(user):
                if user not in sclar:
                    sclar[user] = [float(cost)]
                else:
                    sclar[user].append(float(cost))


# set env
if not os.path.exists(download_folder):
    os.mkdir(download_folder)

if not os.path.exists(unzipped_files):
    os.mkdir(unzipped_files)

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

# calc
threads = []
for file in csv_files:
    threads.append(threading.Thread(target=cost, args=(file,)))
for t in threads:
    t.start()
for t in threads:
    t.join()

# write data
import time
start_time = time.time()
write_cost_data(sclar)
cursor.close()
print("--- %s seconds ---" % (time.time() - start_time))



