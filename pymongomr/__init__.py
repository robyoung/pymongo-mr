__author__="rob"
__date__ ="$Mar 8, 2011 4:23:38 PM$"

from bson.objectid import ObjectId
import pymongo
import multiprocessing
import Queue
import copy
import datetime
import logging
from collections import defaultdict

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger("pymr")
logger.addHandler(NullHandler())

class PoisonPill(object):
    pass

class PoisonQueue(object):
    """
    An iterable queue that will stop iterating once it's received a
    predefined number of stop events.
    """

    def __init__(self, maxsize=None, stops=1):
        self.queue = multiprocessing.Queue(maxsize)
        self.stops = stops

    def __getattr__(self, name):
        return getattr(self.queue, name)

    def done(self):
        self.put(PoisonPill())

    def get_iter(self):
        stops = 0
        while True:
            try:
                item = self.get()
                if isinstance(item, PoisonPill):
                    stops += 1
                    if stops >= self.stops:
                        break
                else:
                    yield item
            except Queue.Empty:
                pass

class MapReduce(object):
    """
    Map reduce base class that manages the worker processes and colates the
    results.
    """

    item_limit    = 100
    reduce_limit  = 1000

    redqueue_size = 10000
    outqueue_size = 10000

    query  = {}
    spec   = {}

    out = None

    def __init__(self, host="localhost", port=27017):
        self._host = host
        self._port = port

#    Connection Methods
    def _get_con(self):
        return pymongo.Connection(self._host, self._port)

    def _get_db(self):
        return self._get_con()[self.database]

#    Override Methods
    def splitter(self):
        """
        Split the input query up into chunks which can be worked on
        independently.
        """
        return [(self.query, [])]

    def init_worker(self, num):
        pass

    def map(self, item):
        """
        Apply to every item and collect the results.
        """
        raise NotImplementedError()

    def reduce(self, key, values):
        """
        Use to combine map results.
        """
        raise NotImplementedError()

    def finalize(self, key, value):
        return value

#    Interface Methods
    def start(self):
        self._start_workers()

        self._outqueue = PoisonQueue(self.outqueue_size)
        if self.out:
            self._get_db()[self.out].drop()
        multiprocessing.Process(target=self._final_reduce).start()

    def join(self):
        self._outqueue.get()

    def results(self):
        if self.out:
            self._outqueue.get()
            coll = self._get_db()[self.out]
            for item in coll.find():
                yield item['_id'], item['value']
        else:
            for item in self._outqueue.get_iter():
                yield item

    def _start_workers(self):
        splits            = self.splitter()
        self._num_workers = len(splits)
        self._redqueue    = PoisonQueue(self.redqueue_size, self._num_workers)

        for num, (query, sort) in enumerate(splits):
            multiprocessing.Process(target=self._worker, args=(num, query, sort)).start()

    def _worker(self, num, query, sort):
        logger.debug("worker %s start" % num)
        self.init_worker(num)
        items = defaultdict(list)
        for item in self._get_db()[self.collection].find(query, self.spec, sort=sort):
            for key, value in self.map(item):
                key = str(key)
                items[key].append(value)
                if len(items[key]) > self.item_limit:
                    items[key] = [self.reduce(key, items[key])]
            if len(items) > self.reduce_limit:
                items = self._reduce_and_send(items, self._redqueue)
                logger.debug("reduce and flush from worker %s" % num)

        self._reduce_and_send(items, self._redqueue)
        self._redqueue.done()

        logger.debug("worker %s finish" % num)

    def _reduce_and_send(self, items, queue):
        for key, values in items.items():
            queue.put((key, self.reduce(key, values)))
        return defaultdict(list)

    def _final_reduce(self):
        items = defaultdict(list)
        for key, value in self._redqueue.get_iter():
            items[key].append(value)

        func = self.out and self._save_func() or self._send_func

        for key, values in items.items():
            func(key, self.finalize(key, self.reduce(key, values)))

        self._outqueue.done()

    def _save_func(self):
        coll = self._get_db()[self.out]
        def func(key, value):
            coll.save({"_id":key, "value":value}, safe=True)
        return func

    def _send_func(self, key, value):
        self._outqueue.put((key, value))
