"""
#!/usr/bin/env python
# -*- coding:utf-8 -*-
@Project : concurrent_pool
@File : concurrent_pool.py
@Author : zhachuan
@Time : 2023/9/19 
"""
import multiprocessing
import random
import threading
import os
import sys
import atexit
import time
import queue
import weakref
from functools import partial
import concurrent.futures
from concurrent.futures import Future, Executor, ProcessPoolExecutor
from multiprocessing.queues import Queue
import traceback

import pdb
class ForkedPdb(pdb.Pdb):
    """
    A Pdb subclass that may be used
    from a forked multiprocessing child
    """
    def interaction(self, *args, **kwargs):
        _stdin = sys.stdin
        try:
            sys.stdin = open('/dev/stdin')
            pdb.Pdb.interaction(self, *args, **kwargs)
        finally:
            sys.stdin = _stdin


_threads_queues = weakref.WeakKeyDictionary()
_threads_wakeups = weakref.WeakKeyDictionary()
_global_shutdown = False

def _python_exit():
    global _global_shutdown
    _global_shutdown = True
    items = list(_threads_wakeups.items())
    for _, thread_wakeup in items:
        # call not protected by ProcessPoolExecutor._shutdown_lock
        thread_wakeup.wakeup()
    for t, _ in items:
        t.join()

atexit.register(_python_exit)

class _RemoteTraceback(Exception):
    def __init__(self, tb):
        self.tb = tb
    def __str__(self):
        return self.tb

class _ExceptionWithTraceback:
    def __init__(self, exc, tb):
        tb = traceback.format_exception(type(exc), exc, tb)
        tb = ''.join(tb)
        self.exc = exc
        # Traceback object needs to be garbage-collected as its frames
        # contain references to all the objects in the exception scope
        self.exc.__traceback__ = None
        self.tb = '\n"""\n%s"""' % tb
    def __reduce__(self):
        return _rebuild_exc, (self.exc, self.tb)

def _rebuild_exc(exc, tb):
    exc.__cause__ = _RemoteTraceback(tb)
    return exc

class _ResultItem(object):
    def __init__(self, work_id, exception=None, result=None):
        self.work_id = work_id
        self.exception = exception
        self.result = result

class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        # noinspection PyBroadException
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            # print(exc)
            # self.logger.exception(f'函数 {self.fn.__name__} 中发生错误，错误原因是 {type(exc)} {exc} ')
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            self = None  # noqa
        else:
            self.future.set_result(result)

    def __str__(self):
        return f'{(self.fn.__name__, self.args, self.kwargs)}'


class _SafeQueue(Queue):
    """Safe Queue set exception to the future object linked to a job"""
    def __init__(self, max_size=0, *, ctx, pending_work_items, shutdown_lock,
                 thread_wakeup):
        self.pending_work_items = pending_work_items
        self.shutdown_lock = shutdown_lock
        self.thread_wakeup = thread_wakeup
        super().__init__(max_size, ctx=ctx)

    def _on_queue_feeder_error(self, e, obj):
        if isinstance(obj, _CallItem):
            tb = traceback.format_exception(type(e), e, e.__traceback__)
            e.__cause__ = _RemoteTraceback('\n"""\n{}"""'.format(''.join(tb)))
            work_item = self.pending_work_items.pop(obj.work_id, None)
            with self.shutdown_lock:
                self.thread_wakeup.wakeup()
            # work_item can be None if another process terminated. In this
            # case, the executor_manager_thread fails all work_items
            # with BrokenProcessPool
            if work_item is not None:
                work_item.future.set_exception(e)
        else:
            super()._on_queue_feeder_error(e, obj)

class SharedCounter(object):
    """ A synchronized shared counter.

    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.

    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/

    """

    def __init__(self, n=0, ctx=None):
        self.count = ctx.Value('i', n)
        # self.count = multiprocessing.Value('i', n)

    def increment(self, n = 1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value


class _Queue(multiprocessing.queues.Queue):
    """ A portable implementation of multiprocessing.Queue.

    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    """

    def __init__(self, max_size=0, ctx=None):
        super().__init__(max_size, ctx=ctx)
        # super().__init__(max_size)
        self.size = SharedCounter(0, ctx=ctx)
        print(self.size)

    def put(self, *args, **kwargs):
        self.size.increment(1)
        super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        self.size.increment(-1)
        return super().get(*args, **kwargs)

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        return not self.qsize()

    def clear(self):
        """ Remove all elements from the Queue. """
        while not self.empty():
            self.get()

def _return_result(call_item, result_queue, future):
    try:
        # print("return result : ", future)
        r = future.result()
    except BaseException as e:
        if _ExceptionWithTraceback is None:
            result_queue.put(_ResultItem(call_item.work_id, exception=e))
        else:
            exc = _ExceptionWithTraceback(e, e.__traceback__)
            result_queue.put(_ResultItem(call_item.work_id, exception=exc))
    else:
        result_queue.put(_ResultItem(call_item.work_id, result=r))
        del r

def _process_worker(max_threads, call_queue, result_queue, thread_pool_executor):
    """Evaluates calls from call_queue and places the results in result_queue.

    This worker is run in a separate process.

    Args:
        call_queue: A multiprocessing.Queue of _CallItems that will be read and
            evaluated by the worker.
        result_queue: A multiprocessing.Queue of _ResultItems that will written
            to by the worker.
        shutdown: A multiprocessing.Event that will be set as a signal to the
            worker that it should exit when call_queue is empty.
    """
    with thread_pool_executor(max_workers=max_threads) as executor:
        while True:
            # Wait for the queue to be empty. Either initial state or a
            # worker got the workitem.

            call_item = call_queue.get(block=True)
            if call_item is None:
                # Wake up queue management thread
                result_queue.put(os.getpid())
                return

            future = executor.submit(call_item.fn,
                                     *call_item.args,
                                     **call_item.kwargs)
            future.add_done_callback(
                partial(_return_result, call_item, result_queue))

            del call_item



class ThreadedProcessPoolExecutor(ProcessPoolExecutor):

    verbose = True

    def __init__(self, max_processes=None, max_threads=None, thread_pool_class=None, verbose=False):

        ThreadedProcessPoolExecutor.verbose = verbose
        if max_processes is None:
            self._max_processes = os.cpu_count() or 1
        else:
            if max_processes <= 0:
                raise ValueError("max_processes must be greater than 0")
            else:
                self._max_processes = max_processes

        if max_threads is None:
            self._max_threads = (self._max_processes or 1) * 5
        else:
            if max_threads <= 0:
                raise ValueError("max_threads must be greater than 0")
            else:
                self._max_threads = max_threads
        if thread_pool_class is None:
            self._thread_pool = ThreadPoolExecutorShrinkAble
        else:
            self._thread_pool = thread_pool_class

        super().__init__(max_workers=self._max_processes)

        queue_size = self._max_processes * self._max_threads
        self._call_queue = _SafeQueue(
            max_size=queue_size, ctx=self._mp_context,
            pending_work_items=self._pending_work_items,
            shutdown_lock=self._shutdown_lock,
            thread_wakeup=self._executor_manager_thread_wakeup)

        if ThreadedProcessPoolExecutor.verbose:
            print("并发池 debug 已开")
            print("最大进程数 : {}  最大线程数 : {}".format(self._max_processes, self._max_threads))
            print("并发池 call_queue 大小为 : ", queue_size)

    def _adjust_process_count(self):

        if self._idle_worker_semaphore.acquire(blocking=False):
            return

        for _ in range(len(self._processes), self._max_workers):
            p = self._mp_context.Process(
                target=_process_worker,
                args=(self._max_threads,
                      self._call_queue,
                      self._result_queue,
                      self._thread_pool))
            p.start()
            if isinstance(self._processes, dict):
                self._processes[p.pid] = p
            else:
                self._processes.add(p)

    def __repr__(self):
        if ThreadedProcessPoolExecutor.verbose:
            print("目前进程池进程情况({}) : ".format(len(self._processes)))
            for k, v in self._processes.items():
                print("{} : {} ".format(k, v))
        return "最大进程数 : {}  最大线程数 : {}".format(self._max_processes, self._max_threads)


class ThreadPoolExecutorShrinkAble(Executor):

    MIN_WORKERS = 5
    KEEP_ALIVE_TIME = 60
    verbose = False

    def __init__(self, max_workers: int = None, thread_name_prefix=''):

        self._max_workers = max_workers or (os.cpu_count() or 1) * 5
        self._thread_name_prefix = thread_name_prefix
        self.work_queue = queue.Queue(10)
        self._threads = weakref.WeakSet()
        self._lock_compute_threads_free_count = threading.Lock()
        self.threads_free_count = 0
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        # self.pool_ident = id(self)
        self.pool_ident = os.getpid()


    def _change_threads_free_count(self, change_num):
        with self._lock_compute_threads_free_count:
            self.threads_free_count += change_num

    def submit(self, fn, *args, **kwargs) -> Future:
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("Thread Pool 已经 shutdown 不能添加新的任务到线程池")
            f = Future()
            w = _WorkItem(f, fn, args, kwargs)
            self.work_queue.put(w)
            self._adjust_thread_count()
            return f

    def _adjust_thread_count(self):
        if self.threads_free_count <= self.MIN_WORKERS and len(self._threads) < self._max_workers:
            t = _CustomThread(self, verbose=ThreadPoolExecutorShrinkAble.verbose)
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self.work_queue

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self.work_queue.put(None)
            # self.work_queue = True
        if wait:
            for t in self._threads:
                t.join()


class _CustomThread(threading.Thread):
    _lock_for_judge_threads_free_count = threading.Lock()
    verbose = False

    def __init__(self, executor: ThreadPoolExecutorShrinkAble, daemon=True, verbose=False):
        super().__init__(daemon=daemon)
        self._executor = executor

    def _remove_thread(self, stop_reason=""):
        self._executor._change_threads_free_count(-1)
        self._executor._threads.remove(self)
        _threads_queues.pop(self)
        if _CustomThread.verbose:
            print("停止线程 {} 触发条件为:\n {}".format(self.ident, stop_reason))

    def run(self):
        if _CustomThread.verbose:
            print("线程池 {} | 启动新线程 {}".format(self._executor.pool_ident, self.ident))
        self._executor._change_threads_free_count(1)
        while True:
            try:
                work_item = self._executor.work_queue.get(block=True, timeout=self._executor.KEEP_ALIVE_TIME)
            except queue.Empty:
                with self._lock_for_judge_threads_free_count:
                    if self._executor.threads_free_count > self._executor.MIN_WORKERS:
                        self._remove_thread(f"线程池({self._executor.pool_ident}) 线程({self.ident}) 超过 {self._executor.KEEP_ALIVE_TIME}s 没有任务, 目前线程数量为{len(self._executor._threads)}")
                        break
                    else:
                        continue
            else:
                if work_item is not None:
                    self._executor._change_threads_free_count(-1)
                    work_item.run()
                    del work_item
                    self._executor._change_threads_free_count(1)
                    continue

                if _global_shutdown or self._executor._shutdown:
                    self._executor.work_queue.put(None)
                    # self._remove_thread(f"线程池({self._executor.pool_ident}) 销毁线程({self.ident})")
                    break


def test(i):
    time.sleep(1)
    print("参数 {} 已经提交".format(i))
    num = 10000
    res = 0
    for n in range(num):
        res += n
    if random.randint(1, 10) < 3:
        # print("扔出错误")
        raise Exception("测试错误")
    else:
        # raise Exception("一直错误")
        return "sleep {}s and res={}".format(i, res)

def main():
    # args : 进程数, 线程数, 线程池类型(一般不用指定)
    # 线程数可以大一点, 因为默认线程池带有可收缩功能, 一段时间休眠线程会自动销毁
    pool = ThreadedProcessPoolExecutor(4, 20, thread_pool_class=thread_pool)
    start = time.time()
    res = None
    with pool as executor:
        futures_list = []
        for i in range(100):
            time.sleep(0.1)
            # 提交任务到并发池
            futures_list.append(executor.submit(test, i))
        # 一次性获取所有结果
        res = concurrent.futures.wait(futures_list, return_when=concurrent.futures.ALL_COMPLETED)
    # 打印结果, res.result() 会把错误也给包装, 就是说如果任务报错了,会通过 result 抛出, 错误可以用 test 函数测试
    map(lambda i: print(i.result()), res)
    print("总用时 : " , time.time() - start)

if __name__ == '__main__':
    # ForkedPdb().set_trace()
    main()