import urllib.request
from time import sleep


def url(year, day):
    return 'https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-{}-{}.csv.zip'.format(year, day)


year = '2016'
dates = ['05', '06', '07', '08']
download_folder = 'download/'

for day in dates:
    urllib.request.urlretrieve(url(year, day), '{}{}-{}.zip'.format(download_folder, year, day))
    sleep(5)

