import urllib.request
from time import sleep
import zipfile


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
