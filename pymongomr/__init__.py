from __future__ import division
from collections import defaultdict
import logging
import math
import multiprocessing
import os
import Queue

import pymongo

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
        for _ in self.get_out_iter():
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

    num_workers   = 1
    num_reducers  = 1

    query  = {}
    spec   = {}

    out = None

    database = None
    collection = None

    _scratch_collection = None

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

    def init_report(self):
        pass

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
        self._scratch_collection = "pymr.scratch.%s" % os.getpid()
        self.init_report()
        self._start_workers()
        self._out().drop()
        self._start_reducers()
        self.complete()
        self._scratch().drop()

    def join(self):
        # nothing to do, legacy
        pass

    def _out(self):
        """Returns the output collection."""
        return self._get_db()[self.out]

    def _scratch(self):
        """Returns the temporary scratch collection."""
        return self._get_db()[self._scratch_collection]

    def results(self):
        for item in self._out().find():
            yield item['_id'], item['value']

    def _start_workers(self):
        self._inqueue     = PoisonQueue(self.inqueue_size, self.num_workers)

        # create a process for each worker
        workers = []
        for num in range(self.num_workers):
            workers.append(multiprocessing.Process(target=self._worker, args=(num,)))

        # start them all
        for worker in workers:
            worker.start()

        # feed them
        for item in self.splitter():
            self._inqueue.put(item)
        self._inqueue.done_all()

        # join them all because the final reducers cannot start until the workers have finished
        for worker in workers:
            worker.join()

    def _find(self, query):
        return self._get_db()[query.collection].find(query.query, query.spec, sort=query.sort, skip=query.skip, limit=query.limit)

    def _worker(self, num):
        logger.debug("worker %s start" % num)
        self.init_worker(num)
        items   = defaultdict(list)
        scratch = self._scratch()

        for query, sort in self._inqueue.get_in_iter():
            logger.debug("worker %s starting split" % num)
            self.query = query

            if not isinstance(query, Query):
                query = Query(self.collection, query, self.spec, sort)

            count = 0
            for item in self._find(query):
                count += 1

                for key, value in self.map(item):
                    key = str(key)
                    items[key].append(value)
                    if len(items[key]) > self.item_limit:
                        items[key] = [self.reduce(key, items[key])]

                if len(items) > self.reduce_limit:
                    items = self._reduce_and_send(items, scratch)
                    logger.debug("reduce and flush from worker %s" % num)

            logger.debug("worker %s finished %s in split" % (num, count))

        self._reduce_and_send(items, scratch)
        logger.debug("worker %s finish" % num)

    def _reduce_and_send(self, items, scratch):
        """Reduce a list of items and append them to the key in the scratch database."""
        for key, values in items.items():
            scratch.update({"_id":key}, {"$push":{"value":self.reduce(key, values)}}, upsert=True)
        return defaultdict(list)

    def _start_reducers(self):
        scratches = self._scratch().count()
        limit     = int(math.ceil(scratches / self.num_reducers))

        # create a process for each reducer
        reducers = []
        for num in range(self.num_reducers):
            reducers.append(multiprocessing.Process(target=self._reducer, args=(num, limit)))

        # start them all
        for reducer in reducers:
            reducer.start()

        # join them all because the scratch collection cannot be dropped before they've all completed
        for reducer in reducers:
            reducer.join()

    def _reducer(self, num, limit):
        self._debug("starting reducer %s" % num)
        scratch = self._scratch()
        cursor  = scratch.find(skip=limit*num, limit=limit, sort=[("_id", pymongo.ASCENDING)])
        save    = self._save_func()
        for key, values in ((doc['_id'], doc['value']) for doc in cursor):
            save(key, self.finalize(key, self.reduce(key, values)))
        self._debug("finished reducer %s" % num)

    def _debug(self, message):
        logging.getLogger("pymr.MapReduce").debug(message)

    def _save_func(self):
        if not self.out:
            raise MapReduceException("No output collection defined, please set a class property called 'out'.")
        coll = self._get_db()[self.out]
        def func(key, value):
            coll.save({"_id":key, "value":value}, safe=True)
        return func

class MapReduceException(Exception):
    pass

class MapReduceWorkerException(MapReduceException):
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

    def __str__(self):
        return "<Query coll=%s, query=%s, spec=%s, sort=%s, skip=%s, limit=%s>" % (self.collection, self.query, self.spec, self.sort, self.skip, self.limit)