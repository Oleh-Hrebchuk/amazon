import os
import re
import csv
import threading
import sqlite3

from queue import Queue
from collections import defaultdict

from download_files import DownloadFiles
from unzip_files import UnzipFiles
from sqlite_manager import create_table


year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'downloads/'
unzipped_files = 'unzipped_files/'
sclar = defaultdict(list)
pattern = re.compile(r'v1:\d+:\d+:\d+:\w+-\w+-\w+-\w+-\w+\Z')


# check_same_thread - to allow threading with SQlite
conn = sqlite3.connect('amazon.sqlite3', check_same_thread=False)
cursor = conn.cursor()


lock = threading.Lock()


def write_cost_data(data, _slice='v1:'):
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
                    key, key[len(_slice):], suma))
            conn.commit()


def cost(file):
    """
    Calculate costs and write them into SQlite with lock context manager
    """
    row_results = {}
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            user = row['user:scalr-meta']
            cost = row['Cost']
            if pattern.match(user):
                if user not in row_results:
                    row_results[user] = [float(cost)]
                else:
                    row_results[user].append(float(cost))

    # according to python docs, there are issues when writing from threads into SQlite. So we need to set lock.
    # https://docs.python.org/3/library/sqlite3.html#multithreading
    with lock:
        write_cost_data(data=row_results)

# set env
if not os.path.exists(download_folder):
    os.mkdir(download_folder)

if not os.path.exists(unzipped_files):
    os.mkdir(unzipped_files)


def prepare():
    """
    Download and unzip files.  
    """

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


def calculate_in_threads(csv_files_list):
    """
    Spawn thread per csv file and join them
    :param csv_files_list:  
    """
    threads = []
    for file in csv_files_list:
        threads.append(threading.Thread(target=cost, args=(file,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    import time

    # call prepare if you want download and unzip files
    # uncomment oif you want to download and unzip

    # prepare()

    create_table()

    csv_files = [unzipped_files + '/' + file for file in os.listdir(unzipped_files)]

    start_time = time.time()

    calculate_in_threads(csv_files_list=csv_files)

    print("--- %s seconds ---" % (time.time() - start_time))