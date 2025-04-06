import logging
import time
import typing
from functools import wraps


def logtime(start: float, action: str) -> None:
    end = time.time()
    logging.debug(f"{action} took {end - start:0.1f} seconds")
    return end


F = typing.TypeVar("F", bound=typing.Callable[..., typing.Any])  # type: ignore


def info_time(func: F) -> F:  # type: ignore
    logger = logging.getLogger("timing." + func.__module__)

    @wraps(func)
    def wrapper(*args, **kwargs):  # type: ignore
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"Function {func.__name__}{args} {kwargs} took {elapsed} seconds")
        return result

    return typing.cast(F, wrapper)  # type: ignore


def debug_time(func: F) -> F:  # type: ignore
    logger = logging.getLogger("timing." + func.__module__)

    @wraps(func)
    def wrapper(*args, **kwargs):  # type: ignore
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.debug(f"Function {func.__name__}{args} {kwargs} took {elapsed} seconds")
        return result

    return typing.cast(F, wrapper)  # type: ignore
