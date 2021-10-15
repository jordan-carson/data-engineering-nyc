import time

from concurrent.futures import ThreadPoolExecutor
from functools import wraps

_DEFAULT_POOL = ThreadPoolExecutor()


def parallel_task(func, executor=None):
    """
    Decorator designed to parallelize any method invocation. We use this by writing @parallel_task before any
    method declaration, and it will be called in parallel without blocking the main thread.

    To fetch the result of this Future Objet, one has to call Future.result(), which will block the main thread until
    the result is fetched.
    @param func: function or method to be called in parallel
    @param executor: Optional ThreadPoolExecutor
    @return: Returns a concurrent.futures.Future instance
    """
    @wraps(func)
    def wrap(*args, **kwargs):
        return (executor or _DEFAULT_POOL).submit(func, *args, **kwargs)
    return wrap


def retry(exception=Exception, value_type=None, tries=3, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.
    Modified from: https://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/

    @param exception:
    @param value_type:
    @param tries:
    @param delay:
    @param backoff:
    @param logger:
    @return:
    """
    def deco_retry(func):
        @wraps(func)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    val = func(*args, **kwargs)
                    if value_type is not None and type(val) != value_type:
                        msg = f'Value should be type {value_type.__name__} - got type {type(val).__name__}'

                        if mtries == 1:
                            raise TypeError(msg)

                        msg += f' - retrying in {mdelay} seconds...'
                    else:
                        return val
                except exception as e:
                    msg = f"{e.__class__.__name__} - {e} retrying in {mdelay} seconds."
                    if mtries == 1:
                        raise e

                if logger:
                    logger.warning(msg)
                else:
                    print(msg)

                time.sleep(mdelay)
                mtries -= 1
                mdelay *= backoff

        return f_retry  # true decorator

    return deco_retry


def timeit(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        print(f"{method.__name__} => {(end_time-start_time)*1000} ms")

        return result

    return wrapper