from typed import Callable
import wrapt


def timeout(*, timeout: float) -> Callable:

    def decorator(fn: Callable) -> Callable:

        @wrapt.decorator
        async def _async_timeout(wrapped, instance, args, kwargs):
            if kwargs["runtime"].total_seconds() >= timeout:
                _handler = (f"{instance.__class__.__name__}.{wrapped.__name__}"
                            if instance else f"{wrapped.__name__}")

                raise kopf.HandlerTimeoutError(
                    f'{_handler} has timed out after {kwargs["runtime"]}.')
            return await wrapped(*args, **kwargs)

        return _async_timeout(fn)

    return decorator
