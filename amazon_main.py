import os
import re
import csv
import zipfile
import urllib.request

from time import sleep


def url(year, day):
    return 'https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-{}-{}.csv.zip'.format(year, day)


year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'downloads/'
unzipped_files = 'unzipped_files'

for day in dates:
    urllib.request.urlretrieve(url(year, day), '{}{}-{}.zip'.format(download_folder, year, day))
    sleep(5)

for day in dates:
    with zipfile.ZipFile('{}{}-{}.zip'.format(download_folder, year, day), "r") as zip_ref:
        zip_ref.extractall("unzipped_files")

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
