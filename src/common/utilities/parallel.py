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
