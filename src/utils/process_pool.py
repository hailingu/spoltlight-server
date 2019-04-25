import threading
import time
import os
import queue
import itertools

from multiprocessing.pool import MaybeEncodingError, RUN, CLOSE, TERMINATE, ApplyResult, ExceptionWithTraceback
from multiprocessing import util, get_context

from utils.task import Task, FlowTask

def _helper_reraises_exception(ex):
    'Pickle-able helper function for use by _guarded_task_generation.'
    raise ex

def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None,
           wrap_exception=False):
    if (maxtasks is not None) and not (isinstance(maxtasks, int)
                                       and maxtasks >= 1):
        raise AssertionError("Maxtasks {!r} is not valid".format(maxtasks))
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            print('process worker try get task')
            task = get()
            print('process worker get task', task)
        except (EOFError, OSError):
            util.debug('worker got EOFError or OSError -- exiting')
            break

        if task is None:
            util.debug('worker got sentinel -- exiting')
            break

        # job, i, func, args, kwds = task
        try:
            # result = (True, func(*args, **kwds))
            print('here')
        except Exception as e:
            # if wrap_exception and func is not _helper_reraises_exception:
            #     e = ExceptionWithTraceback(e, e.__traceback__)
            # result = (False, e)
            print('exception block1')
        try:
            # put((job, i, result))
            print('do nothing')
        except Exception as e:
            # wrapped = MaybeEncodingError(e, result[1])
            # util.debug("Possible encoding error while sending result: %s" % (
            #     wrapped))
            # put((job, i, (False, wrapped)))
            print('exception block2')
        # task = job = result = func = args = kwds = None
        completed += 1
    util.debug('worker exiting after %d tasks' % completed)


class B:
    def __init__(self):
        self.name = 'B'

class ProcessPool:
    '''spotlight process pool'''

    _wrap_exception = True

    def Process(self, *args, **kwds):
        return self._ctx.Process(*args, **kwds)

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, context=None):
        self._ctx = context or get_context()
        self._setup_queues()
        self._taskqueue = queue.SimpleQueue()
        self._cache = {}
        self._state = RUN
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs
        
        if processes is None:
            processes = os.cpu_count() or 1
        if processes < 1:
            raise ValueError("Number of processes must be at least 1")

        if initializer is not None and not callable(initializer):
            raise TypeError('initializer must be a callable')

        self._processes = processes
        self._pool = []
        self._repopulate_pool()

        self._worker_handler = threading.Thread(
            target=ProcessPool._handle_workers,
            args=(self, )
            )

        print(len(self._pool))
        self._worker_handler.daemon = False
        self._worker_handler._state = RUN
        self._worker_handler.start()

        self._task_handler = threading.Thread(
            target=ProcessPool._handle_tasks,
            args=(self._taskqueue, self._inqueue, self._outqueue,
                  self._pool, self._cache)
            )
        self._task_handler.daemon = False
        self._task_handler._state = RUN
        self._task_handler.start()

        # self._result_handler = threading.Thread(
        #     target=ProcessPool._handle_results,
        #     args=(self._outqueue, self._outqueue, self._cache)
        #     )
        # self._result_handler.daemon = False
        # self._result_handler._state = RUN
        # self._result_handler.start()

        # self._terminate = util.Finalize(
        #     self, ProcessPool._terminate_pool,
        #     args=(self._taskqueue, self._inqueue, self._outqueue, self._pool,
        #           self._worker_handler, self._task_handler,
        #           self._result_handler, self._cache),
        #     exitpriority=15
        #     )

    def _setup_queues(self):
        self._inqueue = self._ctx.SimpleQueue()
        self._outqueue = self._ctx.SimpleQueue()
        # self._quick_put = self._inqueue._writer.send
        # self._quick_get = self._outqueue._reader.recv
    
    def _repopulate_pool(self):
        '''
            Bring the number of pool processes up to the specified number,
            for use after reaping workers which have exited.
        '''
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild,
                                   self._wrap_exception)
                            )
            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            # w.daemon = True
            w.start()
            util.debug('added worker')

    def _join_exited_workers(self):
        '''
            Cleanup after any worker processes which have exited due to reaching
            their specified lifetime.  Returns True if any workers were cleaned up.
        '''
        cleaned = False
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            if worker.exitcode is not None:
                # worker exited
                util.debug('cleaning up worker %d' % i)
                worker.join()
                cleaned = True
                del self._pool[i]
        
        return cleaned

    def _maintain_pool(self):
        '''
            Clean up any exited workers and start replacements for them.
        '''

        if self._join_exited_workers():
            self._repopulate_pool()

    def apply_async(self, task, args=(), kwds={}, callback=None,
            error_callback=None):
        '''
        Asynchronous version of `apply()` method.
        '''
        if self._state != RUN:
            raise ValueError("Pool not running")
        result = ApplyResult(self._cache, callback, error_callback)
        self._taskqueue.put(([(result._job, 0, task, args, kwds)], None))
        print(self._taskqueue.qsize())
        return result

    def terminate(self):
        util.debug('terminating pool')
        self._state = TERMINATE
        self._worker_handler._state = TERMINATE
        # self._terminate()

    @staticmethod
    def _handle_tasks(taskqueue, inqueue, outqueue, pool, cache):
        thread = threading.current_thread()

        for taskseq, set_length in iter(taskqueue.get, None):
            task = None
            print('taskqueue', taskqueue.qsize())
            try:
                for task in taskseq:
                    print(task)
                try:
                    inqueue.put(FlowTask(None))
                except Exception as e:
                    print('error', e)
        
            except Exception as e:
                print(e)

        #     try:
        #         print('++++', 'ere0', taskseq)
        #         # iterating taskseq cannot fail
        #         for task in taskseq:
        #             print('+++++', task)
        #             print('state', thread._state)
        #             if thread._state:
        #                 util.debug('task handler found thread._state != RUN')
        #                 break
        #             print('+++++', 'ere', task)
        #             try:
        #                 # inqueue.put(task)
        #                 inqueue.put(FlowTask(None))
        #                 print('------------', inqueue.get())

        #             except Exception as e:
        #                 # job, idx = task[:2]
        #                 # try:
        #                 #     cache[job]._set(idx, (False, e))
        #                 # except KeyError:
        #                 #     pass
        #                 print('exception block0', e)
        #             print('+++++', 'ere1')

        #         else:
        #             print('+++++', 'ere2', set_length)
        #             # if set_length:
        #             #     util.debug('doing set_length()')
        #             #     idx = task[1] if task else -1
        #             #     set_length(idx + 1)
        #             # continue
        #         break
        #     finally:
        #         print('a')
        #         task = taskseq = job = None
        # else:
        #     print('b')
        #     util.debug('task handler got sentinel')

        # print('c')
        # try:
        #     # tell result handler to finish when cache is empty
        #     util.debug('task handler sending sentinel to result handler')
        #     outqueue.put(None)

        #     # tell workers there is no more work
        #     util.debug('task handler sending sentinel to workers')
        #     for p in pool:
        #         inqueue.put(None)
        
        # except OSError:
        #     util.debug('task handler got OSError when sending sentinels')

        # util.debug('task handler exiting')

    @staticmethod
    def _handle_results(outqueue, get, cache):
        thread = threading.current_thread()

        while 1:
            try:
                task = outqueue.get()
            except (OSError, EOFError):
                util.debug('result handler got EOFError/OSError -- exiting')
                return

            if thread._state:
                assert thread._state == TERMINATE, "Thread not in TERMINATE"
                util.debug('result handler found thread._state=TERMINATE')
                break

            if task is None:
                util.debug('result handler got sentinel')
                break

            job, i, obj = task
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass
            task = job = obj = None

        while cache and thread._state != TERMINATE:
            try:
                task = outqueue.get()
            except (OSError, EOFError):
                util.debug('result handler got EOFError/OSError -- exiting')
                return

            if task is None:
                util.debug('result handler ignoring extra sentinel')
                continue
            job, i, obj = task
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass
            task = job = obj = None

        if hasattr(outqueue, '_reader'):
            util.debug('ensuring that outqueue is not full')
            # If we don't make room available in outqueue then
            # attempts to add the sentinel (None) to outqueue may
            # block.  There is guaranteed to be no more than 2 sentinels.
            try:
                for i in range(10):
                    if not outqueue._reader.poll():
                        break
                    get()
            except (OSError, EOFError):
                pass

        util.debug('result handler exiting: len(cache)=%s, thread._state=%s',
              len(cache), thread._state)

    # @staticmethod
    # def _get_tasks(func, it, size):
    #     it = iter(it)
    #     while 1:
    #         x = tuple(itertools.islice(it, size))
    #         if not x:
    #             return
    #         yield (func, x)

    @classmethod
    def _terminate_pool(cls, taskqueue, inqueue, outqueue, pool,
                        worker_handler, task_handler, result_handler, cache):
        # this is guaranteed to only be called once
        util.debug('finalizing pool')

        worker_handler._state = TERMINATE
        task_handler._state = TERMINATE

        util.debug('helping task handler/workers to finish')
        cls._help_stuff_finish(inqueue, task_handler, len(pool))

        if (not result_handler.is_alive()) and (len(cache) != 0):
            raise AssertionError(
                "Cannot have cache with result_hander not alive")

        result_handler._state = TERMINATE
        outqueue.put(None)                  # sentinel

        # We must wait for the worker handler to exit before terminating
        # workers because we don't want workers to be restarted behind our back.
        util.debug('joining worker handler')
        if threading.current_thread() is not worker_handler:
            worker_handler.join()

        # Terminate workers which haven't already finished.
        if pool and hasattr(pool[0], 'terminate'):
            util.debug('terminating workers')
            for p in pool:
                if p.exitcode is None:
                    p.terminate()

        util.debug('joining task handler')
        if threading.current_thread() is not task_handler:
            task_handler.join()

        util.debug('joining result handler')
        if threading.current_thread() is not result_handler:
            result_handler.join()

        if pool and hasattr(pool[0], 'terminate'):
            util.debug('joining pool workers')
            for p in pool:
                if p.is_alive():
                    # worker has not yet exited
                    util.debug('cleaning up worker %d' % p.pid)
                    p.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    @staticmethod
    def _help_stuff_finish(inqueue, task_handler, size):
        # task_handler may be blocked trying to put items on inqueue
        util.debug('removing tasks from inqueue until task handler finished')
        inqueue._rlock.acquire()
        while task_handler.is_alive() and inqueue._reader.poll():
            inqueue._reader.recv()
            time.sleep(0)

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
        util.debug('worker handler exiting')