from typing import Callable, Any
import json
import asyncio
import functools
from functools import partial
import inspect
import sys
import traceback

# For Asyncio Python 3.6 compatibility
if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop
else:
    from asyncio import _get_running_loop as get_running_loop

from ._interface import AbstractAioLockReleaseManager
from ._base import BaseAioHaredisManager
from .._aioclient import AioHaredisClientCmdExecutor


class AioLockReleaseManager(AbstractAioLockReleaseManager):
    """## Aio Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using streams and pub/sub api (For asyncronous execution).
    """

    def __init__(
        self,
        aioharedis_client: AioHaredisClientCmdExecutor = None,
        ):
        """Constructor for RedisLockReleaseManager for Redis as Standalone.

        Args:
            aioharedis_client (AioHaredisClientCmdExecutor): AioHaredisClientCmdExecutor Instance.
        """
        
        self.__rl_manager = BaseAioHaredisManager(
            aioharedis_client=aioharedis_client,
            )
        
    @property
    def rl_manager(self):
        return self.__rl_manager
        
    async def lock_release_with_stream(   
        self,
        func: Callable,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5 * 1000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=0.1,
        redis_availability_strategy="error",
        args=tuple(),
        **kwargs: dict
        ):
        
        # Predefined variables
        result = None
        exception_string = None
        exception_found = False
        func_name = func.__name__
        nullable = [{}, [], "null"]
        tb_str = ""
        
        # Type Checks
        if not isinstance(func, Callable):
            raise TypeError("func must be callable.")
        
        if not isinstance(lock_time_extender_suffix, str):
            raise TypeError("lock_time_extender_suffix must be string.")
                        
        if null_handler not in nullable:
            raise Exception("null_handler must be type of one of these: {nullable}".format(nullable=nullable))
        
        if lock_expire_time < 0:
            raise ValueError("lock_expire_time must be greater than 0.")
        
        # Generate lock key and cache key
        lock_key = await self.rl_manager.lock_key_generator(keys_to_lock, args, kwargs, lock_key_prefix)
        cache_key = lock_key + ".cache"
                    
        # Define stream key for consumer
        consumer_stream_key = "stream:{lock_key}".format(lock_key=lock_key)
        streams = {consumer_stream_key: "$"}
                
        # Print warning messages if run_with_lock_time_extender is False
        if not run_with_lock_time_extender:
            await self.rl_manager.lte_warn(
                lock_time_extender_suffix,
                lock_time_extender_add_time,
                lock_time_extender_blocking_time,
                lock_time_extender_replace_ttl
            )
            
        # Get running event loop if exists, if not, get event loop                     
        try:
            loop = get_running_loop()
        except RuntimeError as err:
            self.rl_manager.redis_logger.debug("Event Loop Error: {err}".format(err=err))
            loop = asyncio.get_event_loop()
        
        # Check if function is coroutine function, if not, check if haredis_client is provided
        is_main_coroutine = inspect.iscoroutinefunction(func)
        
        if redis_availability_strategy not in ("error", "continue"):
            raise Exception("redis_availability_strategy must be one of these: error, continue")
        
        self.rl_manager.redis_logger.debug("Redis availabilty strategy: {redis_availability_strategy}"
                                           .format(redis_availability_strategy=redis_availability_strategy))
        
        is_redis_up = await self.rl_manager.aioharedis_client.is_aioredis_available()
        
        redis_status, conn_exception_msg = is_redis_up
                
        # Decide what to do if redis is not available
        if redis_availability_strategy == "error":
            if redis_status is False:
                raise Exception(conn_exception_msg)
            
        if redis_availability_strategy == "continue":
            if redis_status is False:
                self.rl_manager.redis_logger.warning(conn_exception_msg)
                self.rl_manager.redis_logger.warning("Redis Server is not available. Function {func_name} will be executed without Redis."
                                                     .format(func_name=func_name))
                
                partial_main = partial(func, *args, **kwargs)
                
                # Check if function is coroutine function, if not, run in executor, if yes, run directly
                self.rl_manager.redis_logger.debug("is_main_coroutine: {is_main_coroutine}.".format(is_main_coroutine=is_main_coroutine))
                if is_main_coroutine:
                    runner = await asyncio.gather(partial_main(), loop=loop, return_exceptions=True)
                else:
                    runner = await asyncio.gather(loop.run_in_executor(None, partial_main), return_exceptions=True)
                result = runner[0]
                return result
                                                  
        # Acquire lock
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} will be acquired.".format(lock_key=lock_key))
        lock = await self.rl_manager.aioharedis_client.acquire_lock(lock_key, lock_expire_time)
        is_locked = await self.rl_manager.aioharedis_client.is_locked(lock)
        is_owned = await self.rl_manager.aioharedis_client.is_owned(lock)
        
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} is locked: {is_locked}, is owned: {is_owned}"
                                           .format(lock_key=lock_key, is_locked=is_locked, is_owned=is_owned))
        # If lock is not owned by current process, call consumer otherwise call producer         
        if is_owned:
            try:
                self.rl_manager.redis_logger.info("Lock key: {lock_key} acquired.".format(lock_key=lock_key))
                
                # lock_token = await self.rl_manager.aioharedis_client.client_conn.get(lock_key)
                # print("LOCK TOKEN :", lock_token)
                
                # Get Functions as partial for asyncio.gather or run_sync_async_parallel
                partial_main = await self.rl_manager.partial_main_selector(
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    is_main_coroutine,
                    run_with_lock_time_extender,
                    args, kwargs
                    )

                partial_lock_time_extender = partial(
                    self.rl_manager.run_lte_streams,
                    lock,
                    consumer_stream_key,   
                    lock_time_extender_suffix,
                    lock_time_extender_add_time,
                    lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl
                    )
                
                
                # Define tasks for asyncio.gather
                tasks = [partial_main, partial_lock_time_extender]
                
                # Run function with lock time extender or without lock time extender
                if run_with_lock_time_extender:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed with Lock time extender."
                                                       .format(func_name=func_name))
                    
                    # Check if function is coroutine function, if not, run it with run_sync_async_parallel, if yes, run it with asyncio.gather
                    if is_main_coroutine:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with asyncio.gather."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(tasks[0](), tasks[1](), loop=loop, return_exceptions=False)
                    else:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with run_sync_async_parallel."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            self.rl_manager.run_lte_streams(
                                lock,
                                consumer_stream_key,   
                                lock_time_extender_suffix,
                                lock_time_extender_add_time,
                                lock_time_extender_blocking_time,
                                lock_time_extender_replace_ttl
                            ),
                            return_exceptions=False
                        )
                        
                else:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed without Lock time extender."
                                                       .format(func_name=func_name))
                    if is_main_coroutine:
                        runner = await asyncio.gather(partial_main(), loop=loop, return_exceptions=False)
                    else:
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            return_exceptions=False
                        )
                    
                result = runner[0]
                self.rl_manager.redis_logger.debug("Result of the function: {result}".format(result=result))

            except Exception as e:
                exception_string = e.args[0]
                self.rl_manager.redis_logger.error("Exception: {exception_string}".format(exception_string=exception_string))
                self.rl_manager.redis_logger.error("Exception Occured for Lock key: {lock_key}.".format(lock_key=lock_key))
                self.rl_manager.redis_logger.exception("Task Exception", exc_info=e)
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                tb_str = "".join(tb_str)
                result = exception_string
                exception_found = True
                
            finally:
                                                         
                if exception_string:
                    self.rl_manager.redis_logger.warning("Exception found {exception_string}".format(exception_string=exception_string))

                # Check if result is exception
                if exception_found:
                    self.rl_manager.redis_logger.error("Result is exception. Lock key: {lock_key} will be released. Exception: {result}"
                                                       .format(lock_key=lock_key, result=result))
                    raw_data = "RedException" + ":" + str(result)
                    event_data = {"result": raw_data, "traceback": tb_str, "error": "true"}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.xproduce(stream_name=consumer_stream_key, data=event_data, max_messages=1, maxlen=1, stream_id="*")
                    event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                    event_id = event_info["last-entry"][0]
                    self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))

                    asyncio.ensure_future(self.rl_manager.xdel_event(consumer_stream_key, event_id, event_info, delete_event_wait_time), loop=loop)
                    return event_data
                
                # Check if result is empty
                if result is None or result == {} or result == [] or result == "null":
                    self.rl_manager.redis_logger.warning("Result is empty. Lock key: {lock_key} will be released".format(lock_key=lock_key))
                    raw_data = "null"
                    event_data = {"result": raw_data}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.xproduce(stream_name=consumer_stream_key, data=event_data, max_messages=1, maxlen=1, stream_id="*")
                    event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                    event_id = event_info["last-entry"][0]
                    self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))
                    asyncio.ensure_future(self.rl_manager.xdel_event(consumer_stream_key, event_id, event_info, delete_event_wait_time), loop=loop) 
                    return null_handler
                
                # If everything is ok, serialize data, release lock and finally produce event to consumers
                serialized_result = json.dumps(result)
                event_data = {"result": serialized_result}
                await self.rl_manager.aioharedis_client.release_lock(lock)
                self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                _ = await self.rl_manager.aioharedis_client.xproduce(stream_name=consumer_stream_key, data=event_data, max_messages=1, maxlen=1, stream_id="*")
                event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                event_id = event_info["last-entry"][0]
                # event_data = event_info["last-entry"][1]
                self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))
                asyncio.ensure_future(self.rl_manager.xdel_event(consumer_stream_key, event_id, event_info, delete_event_wait_time), loop=loop) 
        else:
            
            # Call consumer if lock is not owned by current process
            result = await self.rl_manager.xconsume_call(
                lock_key,
                streams,
                consumer_blocking_time,
                null_handler,
                consumer_stream_key,
                consumer_do_retry,
                consumer_retry_count,
                consumer_retry_blocking_time_ms,
                consumer_max_re_retry,
                )
            return result
            
        return result
    
    
    async def lock_release_with_pubsub(   
        self,
        func: Callable,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5 * 1000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        redis_availability_strategy="error",
        args=tuple(),
        **kwargs
        ):
        
        # Predefined variables
        result = None
        exception_string = None
        exception_found = False
        func_name = func.__name__
        nullable = [{}, [], "null"]
        
        # Type Checks
        if not isinstance(func, Callable):
            raise TypeError("func must be callable.")
        
        if not isinstance(lock_time_extender_suffix, str):
            raise TypeError("lock_time_extender_suffix must be string.")
                        
        if null_handler not in nullable:
            raise Exception("null_handler must be type of one of these: {nullable}".format(nullable=nullable))
        
        if lock_expire_time < 0:
            raise ValueError("lock_expire_time must be greater than 0.")
        
        # Generate lock key and cache key
        lock_key = await self.rl_manager.lock_key_generator(keys_to_lock, args, kwargs, lock_key_prefix)
        cache_key = lock_key + ".cache"
                    
        # Define pub-sub channel for consumer
        pubsub_channel = "pubsub:{lock_key}".format(lock_key=lock_key)
                
        # Print warning messages if run_with_lock_time_extender is False
        if not run_with_lock_time_extender:
            await self.rl_manager.lte_warn(
                lock_time_extender_suffix,
                lock_time_extender_add_time,
                lock_time_extender_blocking_time,
                lock_time_extender_replace_ttl
            )
            
        # Get running event loop if exists, if not, get event loop                     
        try:
            loop = get_running_loop()
        except RuntimeError as err:
            self.rl_manager.redis_logger.debug("Event Loop Error: {err}".format(err=err))
            loop = asyncio.get_event_loop()
        
        # Check if function is coroutine function, if not, check if haredis_client is provided
        is_main_coroutine = inspect.iscoroutinefunction(func)
        if not is_main_coroutine and not self.rl_manager.aioharedis_client:
            raise RuntimeError("haredis_client parameter must be provided in class constructor for syncronous execution.")
        
        if redis_availability_strategy not in ("error", "continue"):
            raise Exception("redis_availability_strategy must be one of these: error, continue")
        
        self.rl_manager.redis_logger.debug("Redis availabilty strategy: {redis_availability_strategy}"
                                           .format(redis_availability_strategy=redis_availability_strategy))
        
        is_redis_up = await self.rl_manager.aioharedis_client.is_aioredis_available()
        
        redis_status, conn_exception_msg = is_redis_up
                
        # Decide what to do if redis is not available
        if redis_availability_strategy == "error":
            if redis_status is False:
                raise Exception(conn_exception_msg)
            
        if redis_availability_strategy == "continue":
            if redis_status is False:
                self.rl_manager.redis_logger.warning(conn_exception_msg)
                self.rl_manager.redis_logger.warning("Redis Server is not available. Function {func_name} will be executed without Redis."
                                                     .format(func_name=func_name))
                
                partial_main = partial(func, *args, **kwargs)
                
                # Check if function is coroutine function, if not, run in executor, if yes, run directly
                self.rl_manager.redis_logger.debug("is_main_coroutine: {is_main_coroutine}.".format(is_main_coroutine=is_main_coroutine))
                if is_main_coroutine:
                    runner = await asyncio.gather(partial_main(), loop=loop, return_exceptions=True)
                else:
                    runner = await asyncio.gather(loop.run_in_executor(None, partial_main), return_exceptions=True)
                result = runner[0]
                return result
                                       
        # Acquire lock
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} will be acquired.".format(lock_key=lock_key))
        lock = await self.rl_manager.aioharedis_client.acquire_lock(lock_key, lock_expire_time)
        is_locked = await self.rl_manager.aioharedis_client.is_locked(lock)
        is_owned = await self.rl_manager.aioharedis_client.is_owned(lock)
        
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} is locked: {is_locked}, is owned: {is_owned}"
                                           .format(lock_key=lock_key, is_locked=is_locked, is_owned=is_owned))
        
        # If lock is not owned by current process, call consumer otherwise call producer         
        if is_owned:
            try:
                self.rl_manager.redis_logger.info("Lock key: {lock_key} acquired.".format(lock_key=lock_key))
                
                partial_main = await self.rl_manager.partial_main_selector_pubsub(
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    is_main_coroutine,
                    run_with_lock_time_extender,
                    args, kwargs
                    )

                partial_lock_time_extender = partial(
                    self.rl_manager.run_lte_pubsub,
                    lock,
                    pubsub_channel,   
                    lock_time_extender_suffix,
                    lock_time_extender_add_time,
                    lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl
                    )
                
                # Define tasks for asyncio.gather
                tasks = [partial_main, partial_lock_time_extender]
                
                # Run function with lock time extender or without lock time extender
                if run_with_lock_time_extender:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed with Lock time extender."
                                                       .format(func_name=func_name))
                    
                    # Check if function is coroutine function, if not, run it with run_sync_async_parallel, if yes, run it with asyncio.gather
                    if is_main_coroutine:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with asyncio.gather."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(tasks[0](), tasks[1](), loop=loop, return_exceptions=False)
                    else:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with run_sync_async_parallel."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            self.rl_manager.run_lte_pubsub(
                                lock,
                                pubsub_channel,   
                                lock_time_extender_suffix,
                                lock_time_extender_add_time,
                                lock_time_extender_blocking_time,
                                lock_time_extender_replace_ttl
                            ),
                            return_exceptions=False
                        )
                        
                else:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed without Lock time extender."
                                                       .format(func_name=func_name))
                    if is_main_coroutine:
                        runner = await asyncio.gather(partial_main(), loop=loop, return_exceptions=False)
                    else:
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            return_exceptions=False
                        )
                    
                result = runner[0]
                self.rl_manager.redis_logger.debug("Result of the function: {result}".format(result=result))

            except Exception as e:
                exception_string = e.args[0]
                self.rl_manager.redis_logger.error("Exception: {exception_string}".format(exception_string=exception_string))
                self.rl_manager.redis_logger.error("Exception Occured for Lock key: {lock_key}.".format(lock_key=lock_key))
                self.rl_manager.redis_logger.exception("Task Exception", exc_info=e)
                result = exception_string
                exception_found = True
                
            finally:
                                                         
                if exception_string:
                    self.rl_manager.redis_logger.warning("Exception found {exception_string}".format(exception_string=exception_string))

                # Check if result is exception
                if exception_found:
                    self.rl_manager.redis_logger.error("Result is exception. Lock key: {lock_key} will be released. Exception: {result}"
                                                       .format(lock_key=lock_key, result=result))
                    raw_data = "RedException" + ":" + str(result)
                    # event_data = {"result": raw_data}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.publish_msg(pubsub_channel=pubsub_channel, message=raw_data)
                    self.rl_manager.redis_logger.info("Event produced to notify consumers, pubsub-channel: {pubsub_channel} Event: {message}"
                                        .format(pubsub_channel=pubsub_channel, message=_))

                    return raw_data
                
                # Check if result is empty
                if result is None or result == {} or result == [] or result == "null":
                    self.rl_manager.redis_logger.warning("Result is empty. Lock key: {lock_key} will be released".format(lock_key=lock_key))
                    raw_data = "null"
                    # event_data = {"result": raw_data}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.publish_msg(pubsub_channel=pubsub_channel, message=raw_data)
                    self.rl_manager.redis_logger.info("Event produced to notify consumers, pubsub-channel: {pubsub_channel} Event: {message}"
                                        .format(pubsub_channel=pubsub_channel, message=_))
                    return null_handler
                
                # If everything is ok, serialize data, release lock and finally produce event to consumers
                serialized_result = json.dumps(result)
                # event_data = {"result": serialized_result}
                await self.rl_manager.aioharedis_client.release_lock(lock)
                self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                _ = await self.rl_manager.aioharedis_client.publish_msg(pubsub_channel=pubsub_channel, message=serialized_result)
                self.rl_manager.redis_logger.info("Event produced to notify consumers, pubsub-channel: {pubsub_channel} Event: {message}"
                                    .format(pubsub_channel=pubsub_channel, message=_))
                
        else:
            
            # Call consumer if lock is not owned by current process
            result = await self.rl_manager.subscriber_call(
                lock_key,
                pubsub_channel,
                consumer_blocking_time,
                null_handler,
                consumer_do_retry,
                consumer_retry_count,
                consumer_retry_blocking_time_ms,
                consumer_max_re_retry,
                )
            return result
            
        return result
                        
    def lock_release_decorator_streams(
        self,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
    ) -> Any:

        def decorator(func: Callable):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                result = await self.lock_release_with_stream(
                    func=func,
                    keys_to_lock=keys_to_lock,
                    lock_key_prefix=lock_key_prefix,
                    lock_expire_time=lock_expire_time,
                    consumer_blocking_time=consumer_blocking_time,
                    consumer_do_retry=consumer_do_retry,
                    consumer_retry_count=consumer_retry_count,
                    consumer_retry_blocking_time_ms=consumer_retry_blocking_time_ms,
                    consumer_max_re_retry=consumer_max_re_retry,
                    null_handler=null_handler,
                    run_with_lock_time_extender=run_with_lock_time_extender,
                    lock_time_extender_suffix=lock_time_extender_suffix,
                    lock_time_extender_add_time=lock_time_extender_add_time,
                    lock_time_extender_blocking_time=lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl=lock_time_extender_replace_ttl,
                    delete_event_wait_time=delete_event_wait_time,
                    redis_availability_strategy=redis_availability_strategy,
                    args=args,
                    **kwargs
                )
                return result
            
            return async_wrapper
        
        return decorator
    
    def aio_lock_release_decorator_pubsub(
        self,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5000,
        lock_time_extender_replace_ttl=True,
        redis_availability_strategy="error",
    ) -> Any:

        def decorator(func: Callable):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                result = await self.lock_release_with_pubsub(
                    func=func,
                    keys_to_lock=keys_to_lock,
                    lock_key_prefix=lock_key_prefix,
                    lock_expire_time=lock_expire_time,
                    consumer_blocking_time=consumer_blocking_time,
                    consumer_do_retry=consumer_do_retry,
                    consumer_retry_count=consumer_retry_count,
                    consumer_retry_blocking_time_ms=consumer_retry_blocking_time_ms,
                    consumer_max_re_retry=consumer_max_re_retry,
                    null_handler=null_handler,
                    run_with_lock_time_extender=run_with_lock_time_extender,
                    lock_time_extender_suffix=lock_time_extender_suffix,
                    lock_time_extender_add_time=lock_time_extender_add_time,
                    lock_time_extender_blocking_time=lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl=lock_time_extender_replace_ttl,
                    redis_availability_strategy=redis_availability_strategy,
                    args=args,
                    **kwargs
                )
                return result
            
            return async_wrapper
        
        return decorator