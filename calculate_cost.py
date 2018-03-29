import threading
import re
import csv

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
