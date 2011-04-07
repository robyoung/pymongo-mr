__author__="rob"
__date__ ="$Mar 8, 2011 4:23:38 PM$"

import pymongo
import multiprocessing
import Queue
import logging
import os
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

    def done_all(self):
        for num in range(self.stops):
            self.done()

    def join(self):
        """Wait for all poison pills ignoring anything else."""
        for event in self.get_out_iter():
            pass


    def get_out_iter(self):
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

    def get_in_iter(self):
        while True:
            try:
                item = self.get()
                if isinstance(item, PoisonPill):
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

    item_limit    = 1000
    reduce_limit  = 100

    inqueue_size  = 100
    redqueue_size = 100
    outqueue_size = 100

    num_workers   = 1

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
        """
        Called once for each key, use to calculate statistics over a specific
        aggregation.
        """
        return value

    def complete(self):
        """
        Called when all finalize calls have been completed, use to calculate
        statistics over the entire result.
        """

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
            for item in self._outqueue.get_out_iter():
                yield item

    def _start_workers(self):
        self._inqueue     = PoisonQueue(self.inqueue_size, self.num_workers)
        self._redqueue    = PoisonQueue(self.redqueue_size, self.num_workers)

        for num in range(self.num_workers):
            multiprocessing.Process(target=self._worker, args=(num,)).start()

        for item in self.splitter():
            self._inqueue.put(item)

        self._inqueue.done_all()

    def _find(self, query):
        return self._get_db()[query.collection].find(query.query, query.spec, sort=query.sort, skip=query.skip, limit=query.limit)

    def _worker(self, num):
        logger.debug("worker %s start" % num)
        self.init_worker(num)
        items = defaultdict(list)

        for query, sort in self._inqueue.get_in_iter():
            try:
                logger.debug("worker %s starting split" % num)
                self.query = query

                if not isinstance(query, Query):
                    query = Query(self.collection, query, self.spec, sort)

                find  = self._find(query)
                count = 0
                for item in find:
                    count += 1
                    for key, value in self.map(item):
                        key = str(key)
                        items[key].append(value)
                        if len(items[key]) > self.item_limit:

                            items[key] = [self.reduce(key, items[key])]
                    if len(items) > self.reduce_limit:
                        items = self._reduce_and_send(items, self._redqueue)
                        logger.debug("reduce and flush from worker %s" % num)

                logger.debug("worker %s finished %s in split" % (num, count))
            except Exception, e:
                raise MapReduceException(query.collection, query.query, query.sort, e)

        self._reduce_and_send(items, self._redqueue)
        self._redqueue.done()
        logger.debug("worker %s finish" % num)

    def _reduce_and_send(self, items, queue):
        for key, values in items.items():
            queue.put((key, self.reduce(key, values)))
        return defaultdict(list)

    def _final_reduce(self):
        self._debug("starting final reduce")
        scratch = self._get_db()["pymr.scratch.%s" % os.getpid()]
        for key, value in self._redqueue.get_out_iter():
            scratch.update({"_id":key}, {"$push":{"value":value}}, upsert=True)

        # TODO: decide whether this is a good idea, should they always go
        #       output collection?
        func = self.out and self._save_func() or self._send_func

        for key, values in (doc['_id'], doc['value'] for doc in scratch.find()):
            func(key, self.finalize(key, self.reduce(key, values)))

        self._get_db().drop_collection(scratch.name)

        self.complete()
        self._outqueue.done()
        self._debug("final reduce complete")

    def _save_func(self):
        coll = self._get_db()[self.out]
        def func(key, value):
            coll.save({"_id":key, "value":value}, safe=True)
        return func

    def _send_func(self, key, value):
        self._outqueue.put((key, value))

class MapReduceException(Exception):
    def __init__(self, collection, query, sort, e):
        super(MapReduceException, self).__init__("%s, %s, %s, %s" % (collection, query, sort, e.message))
        self.collection = collection
        self.query      = query
        self.sort       = sort
        self.e          = e

class Query(object):
    def __init__(self, collection, query, spec, sort, skip=0, limit=0):
        self.collection = collection
        self.query      = query
        self.spec       = spec
        self.sort       = sort
        self.skip       = skip
        self.limit      = limit

    def __iter__(self):
        yield self
        yield None