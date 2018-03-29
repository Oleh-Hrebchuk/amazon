import os
import re
import csv
import zipfile
import urllib.request
from time import sleep
import datetime
import asyncio

year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'downloads/'
unzipped_files = 'unzipped_files'


async def download_files(year, day, place):
    url = 'https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-{}-{}.csv.zip'.format(year, day)
    urllib.request.urlretrieve(url, '{}{}-{}.zip'.format(place, year, day))

    return 'Downloaded file'


async def main(dates_list):
    coroutines = [download_files(year, day, download_folder) for day in dates_list]
    completed, pending = await  asyncio.wait(coroutines)
    for item in completed:
        print(item.result())


if __name__ == '__main__':
    print(datetime.datetime.now())
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(dates))
    finally:
        event_loop.close()
    print(datetime.datetime.now())
