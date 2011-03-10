__author__="rob"
__date__ ="$Mar 8, 2011 4:24:06 PM$"

import pymongo
import pymongomr
import datetime
import logging
import sys
import copy

logger = logging.getLogger()

class MyMapReduce(pymongomr.MapReduce):
    database   = "analytics"
    collection = "events"

    query      = {
        "type": "Event.WebRequest",
        "created_at": {
            "$gt": datetime.datetime(2011, 2, 5)
        }
    }

    spec       = {
        "created_at": True,
        "user_agent": True
    }

    def splitter(self):
        num_workers = 6
        delta    = datetime.datetime.now() - self.query['created_at']['$gt']
        delta   /= num_workers
        queries  = []
        for i in range(num_workers):
            query = copy.deepcopy(self.query)
            query['created_at']['$gt']  = self.query['created_at']['$gt'] + delta * i
            query['created_at']['$lte'] = self.query['created_at']['$gt'] + delta * (i + 1)
            queries.append((query, [("created_at", pymongo.ASCENDING)]))
        del queries[-1][0]['created_at']['$lte']

        return queries

    def map(self, item):
        detail = {}
        if "Googlebot" in item['user_agent']:
            detail['Googlebot'] = 1
        elif "AdsBot" in item['user_agent']:
            detail['AdsBot'] = 1
        elif "Feedfetcher" in item['user_agent']:
            detail['Feedfetcher'] = 1

        if detail:
            yield item['created_at'].replace(hour=0, minute=0, second=0), detail

    def reduce(self, key, values):
        details = {}
        for item in values:
            for key, value in item.items():
                details.setdefault(key, 0)
                details[key] += value
        return details

if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    mymr = MyMapReduce(host="localhost")
    coll = mymr.start()
    for key, value in mymr.results():
        print key, value
