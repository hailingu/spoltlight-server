import multiprocessing
import queue
import time
import threading
import os


RUN = 1
CLOSE = 0
TERMINATE = 2

class ProcessPool(object):
    '''
    Class which supports an async version of applying functions to arguments.
    '''
    _wrap_exception = True

    def Process(self, *args, **kwds):
        return self._ctx.Process(*args, **kwds)

    def __init__(self, processes=None, maxtasks=None):
        self._ctx = multiprocessing.get_context()
        self._setup_queues()
        self._taskqueue = queue.SimpleQueue()
        self._cache = {}
        self._state = RUN
        self._maxtasks = maxtasks

        if processes is None:
            processes = os.cpu_count() or 1
        if processes < 1:
            raise ValueError("Number of processes must be at least 1")

        self._processes = processes
        self._pool = []
        self._repopulate_pool()

        self._worker_handler = threading.Thread(
            target=ProcessPool._handle_workers,
            args=(self, )
            )
        
        self._worker_handler.daemon = True
        self._worker_handler._state = RUN
        self._worker_handler.start()

        self._task_handler = threading.Thread(
            target=ProcessPool._handle_tasks,
            args=(self._taskqueue, self._quick_put, self._outqueue,
                  self._pool, self._cache)
            )
        # self._task_handler.daemon = True
        # self._task_handler._state = RUN
        # self._task_handler.start()

        # self._result_handler = threading.Thread(
        #     target=Pool._handle_results,
        #     args=(self._outqueue, self._quick_get, self._cache)
        #     )
        # self._result_handler.daemon = True
        # self._result_handler._state = RUN
        # self._result_handler.start()

        # self._terminate = util.Finalize(
        #     self, self._terminate_pool,
        #     args=(self._taskqueue, self._inqueue, self._outqueue, self._pool,
        #           self._worker_handler, self._task_handler,
        #           self._result_handler, self._cache),
        #     exitpriority=15
        #     )

    # def _join_exited_workers(self):
    #     """Cleanup after any worker processes which have exited due to reaching
    #     their specified lifetime.  Returns True if any workers were cleaned up.
    #     """
    #     cleaned = False
    #     for i in reversed(range(len(self._pool))):
    #         worker = self._pool[i]
    #         if worker.exitcode is not None:
    #             # worker exited
    #             util.debug('cleaning up worker %d' % i)
    #             worker.join()
    #             cleaned = True
    #             del self._pool[i]
    #     return cleaned

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue, self._maxtasks)
                            )
            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()

    # def _maintain_pool(self):
    #     """Clean up any exited workers and start replacements for them.
    #     """
    #     if self._join_exited_workers():
    #         self._repopulate_pool()

    def _setup_queues(self):
        self._inqueue = self._ctx.SimpleQueue()
        self._outqueue = self._ctx.SimpleQueue()
        self._quick_put = self._inqueue._writer.send
        self._quick_get = self._outqueue._reader.recv

    # def apply(self, func, args=(), kwds={}):
    #     '''
    #     Equivalent of `func(*args, **kwds)`.
    #     Pool must be running.
    #     '''
    #     return self.apply_async(func, args, kwds).get()

    # def map(self, func, iterable, chunksize=None):
    #     '''
    #     Apply `func` to each element in `iterable`, collecting the results
    #     in a list that is returned.
    #     '''
    #     return self._map_async(func, iterable, mapstar, chunksize).get()

    # def starmap(self, func, iterable, chunksize=None):
    #     '''
    #     Like `map()` method but the elements of the `iterable` are expected to
    #     be iterables as well and will be unpacked as arguments. Hence
    #     `func` and (a, b) becomes func(a, b).
    #     '''
    #     return self._map_async(func, iterable, starmapstar, chunksize).get()

    # def starmap_async(self, func, iterable, chunksize=None, callback=None,
    #         error_callback=None):
    #     '''
    #     Asynchronous version of `starmap()` method.
    #     '''
    #     return self._map_async(func, iterable, starmapstar, chunksize,
    #                            callback, error_callback)

    # def _guarded_task_generation(self, result_job, func, iterable):
    #     '''Provides a generator of tasks for imap and imap_unordered with
    #     appropriate handling for iterables which throw exceptions during
    #     iteration.'''
    #     try:
    #         i = -1
    #         for i, x in enumerate(iterable):
    #             yield (result_job, i, func, (x,), {})
    #     except Exception as e:
    #         yield (result_job, i+1, _helper_reraises_exception, (e,), {})

    # def imap(self, func, iterable, chunksize=1):
    #     '''
    #     Equivalent of `map()` -- can be MUCH slower than `Pool.map()`.
    #     '''
    #     if self._state != RUN:
    #         raise ValueError("Pool not running")
    #     if chunksize == 1:
    #         result = IMapIterator(self._cache)
    #         self._taskqueue.put(
    #             (
    #                 self._guarded_task_generation(result._job, func, iterable),
    #                 result._set_length
    #             ))
    #         return result
    #     else:
    #         if chunksize < 1:
    #             raise ValueError(
    #                 "Chunksize must be 1+, not {0:n}".format(
    #                     chunksize))
    #         task_batches = Pool._get_tasks(func, iterable, chunksize)
    #         result = IMapIterator(self._cache)
    #         self._taskqueue.put(
    #             (
    #                 self._guarded_task_generation(result._job,
    #                                               mapstar,
    #                                               task_batches),
    #                 result._set_length
    #             ))
    #         return (item for chunk in result for item in chunk)

    # def imap_unordered(self, func, iterable, chunksize=1):
    #     '''
    #     Like `imap()` method but ordering of results is arbitrary.
    #     '''
    #     if self._state != RUN:
    #         raise ValueError("Pool not running")
    #     if chunksize == 1:
    #         result = IMapUnorderedIterator(self._cache)
    #         self._taskqueue.put(
    #             (
    #                 self._guarded_task_generation(result._job, func, iterable),
    #                 result._set_length
    #             ))
    #         return result
    #     else:
    #         if chunksize < 1:
    #             raise ValueError(
    #                 "Chunksize must be 1+, not {0!r}".format(chunksize))
    #         task_batches = Pool._get_tasks(func, iterable, chunksize)
    #         result = IMapUnorderedIterator(self._cache)
    #         self._taskqueue.put(
    #             (
    #                 self._guarded_task_generation(result._job,
    #                                               mapstar,
    #                                               task_batches),
    #                 result._set_length
    #             ))
    #         return (item for chunk in result for item in chunk)

    # def apply_async(self, func, args=(), kwds={}, callback=None,
    #         error_callback=None):
    #     '''
    #     Asynchronous version of `apply()` method.
    #     '''
    #     if self._state != RUN:
    #         raise ValueError("Pool not running")
    #     result = ApplyResult(self._cache, callback, error_callback)
    #     self._taskqueue.put(([(result._job, 0, func, args, kwds)], None))
    #     return result

    # def map_async(self, func, iterable, chunksize=None, callback=None,
    #         error_callback=None):
    #     '''
    #     Asynchronous version of `map()` method.
    #     '''
    #     return self._map_async(func, iterable, mapstar, chunksize, callback,
    #         error_callback)

    # def _map_async(self, func, iterable, mapper, chunksize=None, callback=None,
    #         error_callback=None):
    #     '''
    #     Helper function to implement map, starmap and their async counterparts.
    #     '''
    #     if self._state != RUN:
    #         raise ValueError("Pool not running")
    #     if not hasattr(iterable, '__len__'):
    #         iterable = list(iterable)

    #     if chunksize is None:
    #         chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
    #         if extra:
    #             chunksize += 1
    #     if len(iterable) == 0:
    #         chunksize = 0

    #     task_batches = Pool._get_tasks(func, iterable, chunksize)
    #     result = MapResult(self._cache, chunksize, len(iterable), callback,
    #                        error_callback=error_callback)
    #     self._taskqueue.put(
    #         (
    #             self._guarded_task_generation(result._job,
    #                                           mapper,
    #                                           task_batches),
    #             None
    #         )
    #     )
    #     return result

    @staticmethod
    def _handle_workers(pool):
        thread = threading.current_thread()

        # Keep maintaining workers until the cache gets drained, unless the pool
        # is terminated.
        while thread._state == RUN or (pool._cache and thread._state != TERMINATE):
            pool._maintain_pool()
            time.sleep(0.1)
        # send sentinel to stop workers
        pool._taskqueue.put(None)
    
    @staticmethod
    def _handle_tasks(taskqueue, put, outqueue, pool, cache):
        thread = threading.current_thread()

        for taskseq, set_length in iter(taskqueue.get, None):
            task = None
            try:
                # iterating taskseq cannot fail
                for task in taskseq:
                    if thread._state:
                        print('task handler found thread._state != RUN')
                        break
                    try:
                        put(task)
                    except Exception as e:
                        job, idx = task[:2]
                        try:
                            cache[job]._set(idx, (False, e))
                        except KeyError:
                            pass
                else:
                    if set_length:
                        print('doing set_length()')
                        idx = task[1] if task else -1
                        set_length(idx + 1)
                    continue
                break
            finally:
                task = taskseq = job = None
        else:
            print('task handler got sentinel')

        try:
            # tell result handler to finish when cache is empty
            print('task handler sending sentinel to result handler')
            outqueue.put(None)

            # tell workers there is no more work
            print('task handler sending sentinel to workers')
            for p in pool:
                put(None)
        except OSError:
            print('task handler got OSError when sending sentinels')

        print('task handler exiting')

    # @staticmethod
    # def _handle_results(outqueue, get, cache):
    #     thread = threading.current_thread()

    #     while 1:
    #         try:
    #             task = get()
    #         except (OSError, EOFError):
    #             util.debug('result handler got EOFError/OSError -- exiting')
    #             return

    #         if thread._state:
    #             assert thread._state == TERMINATE, "Thread not in TERMINATE"
    #             util.debug('result handler found thread._state=TERMINATE')
    #             break

    #         if task is None:
    #             util.debug('result handler got sentinel')
    #             break

    #         job, i, obj = task
    #         try:
    #             cache[job]._set(i, obj)
    #         except KeyError:
    #             pass
    #         task = job = obj = None

    #     while cache and thread._state != TERMINATE:
    #         try:
    #             task = get()
    #         except (OSError, EOFError):
    #             util.debug('result handler got EOFError/OSError -- exiting')
    #             return

    #         if task is None:
    #             util.debug('result handler ignoring extra sentinel')
    #             continue
    #         job, i, obj = task
    #         try:
    #             cache[job]._set(i, obj)
    #         except KeyError:
    #             pass
    #         task = job = obj = None

    #     if hasattr(outqueue, '_reader'):
    #         util.debug('ensuring that outqueue is not full')
    #         # If we don't make room available in outqueue then
    #         # attempts to add the sentinel (None) to outqueue may
    #         # block.  There is guaranteed to be no more than 2 sentinels.
    #         try:
    #             for i in range(10):
    #                 if not outqueue._reader.poll():
    #                     break
    #                 get()
    #         except (OSError, EOFError):
    #             pass

    #     util.debug('result handler exiting: len(cache)=%s, thread._state=%s',
    #           len(cache), thread._state)

    # @staticmethod
    # def _get_tasks(func, it, size):
    #     it = iter(it)
    #     while 1:
    #         x = tuple(itertools.islice(it, size))
    #         if not x:
    #             return
    #         yield (func, x)

    # def __reduce__(self):
    #     raise NotImplementedError(
    #           'pool objects cannot be passed between processes or pickled'
    #           )

    # def close(self):
    #     util.debug('closing pool')
    #     if self._state == RUN:
    #         self._state = CLOSE
    #         self._worker_handler._state = CLOSE

    # def terminate(self):
    #     util.debug('terminating pool')
    #     self._state = TERMINATE
    #     self._worker_handler._state = TERMINATE
    #     self._terminate()

    # def join(self):
    #     util.debug('joining pool')
    #     if self._state == RUN:
    #         raise ValueError("Pool is still running")
    #     elif self._state not in (CLOSE, TERMINATE):
    #         raise ValueError("In unknown state")
    #     self._worker_handler.join()
    #     self._task_handler.join()
    #     self._result_handler.join()
    #     for p in self._pool:
    #         p.join()

    # @staticmethod
    # def _help_stuff_finish(inqueue, task_handler, size):
    #     # task_handler may be blocked trying to put items on inqueue
    #     util.debug('removing tasks from inqueue until task handler finished')
    #     inqueue._rlock.acquire()
    #     while task_handler.is_alive() and inqueue._reader.poll():
    #         inqueue._reader.recv()
    #         time.sleep(0)

    # @classmethod
    # def _terminate_pool(cls, taskqueue, inqueue, outqueue, pool,
    #                     worker_handler, task_handler, result_handler, cache):
    #     # this is guaranteed to only be called once
    #     util.debug('finalizing pool')

    #     worker_handler._state = TERMINATE
    #     task_handler._state = TERMINATE

    #     util.debug('helping task handler/workers to finish')
    #     cls._help_stuff_finish(inqueue, task_handler, len(pool))

    #     if (not result_handler.is_alive()) and (len(cache) != 0):
    #         raise AssertionError(
    #             "Cannot have cache with result_hander not alive")

    #     result_handler._state = TERMINATE
    #     outqueue.put(None)                  # sentinel

    #     # We must wait for the worker handler to exit before terminating
    #     # workers because we don't want workers to be restarted behind our back.
    #     util.debug('joining worker handler')
    #     if threading.current_thread() is not worker_handler:
    #         worker_handler.join()

    #     # Terminate workers which haven't already finished.
    #     if pool and hasattr(pool[0], 'terminate'):
    #         util.debug('terminating workers')
    #         for p in pool:
    #             if p.exitcode is None:
    #                 p.terminate()

    #     util.debug('joining task handler')
    #     if threading.current_thread() is not task_handler:
    #         task_handler.join()

    #     util.debug('joining result handler')
    #     if threading.current_thread() is not result_handler:
    #         result_handler.join()

    #     if pool and hasattr(pool[0], 'terminate'):
    #         util.debug('joining pool workers')
    #         for p in pool:
    #             if p.is_alive():
    #                 # worker has not yet exited
    #                 util.debug('cleaning up worker %d' % p.pid)
    #                 p.join()

    # def __enter__(self):
    #     return self

    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.terminate()


def worker(inqueue, outqueue, maxtasks=None):
    if (maxtasks is not None) and not (isinstance(maxtasks, int)
                                       and maxtasks >= 1):
        raise AssertionError("Maxtasks {!r} is not valid".format(maxtasks))
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            task = get()
        except (EOFError, OSError):
            break

        if task is None:
            break

        job, i, func = task
        try:
            result = func()
        except Exception as e:
            result = (False, e)
        try:
            put((job, i, result))
        except Exception as e:
            put((job, i, (False, e)))

        task = job = result = func = None
        completed += 1
