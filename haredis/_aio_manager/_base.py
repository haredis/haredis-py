"""Implementation of Distributed Lock-Release Algorithm for Distributed Caching Locking in Redis"""

from typing import Callable, Union, Any
import logging
import json
import asyncio
from functools import partial

from redis.asyncio.lock import Lock

from ._exceptions import NotCoroError
from ._interface import AbstractBaseAioHaredisManager
from .._aioclient import AioHaredisClientCmdExecutor 



class BaseAioHaredisManager(AbstractBaseAioHaredisManager):
    """## Base Aio Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using streams and pub/sub api (For asyncronous execution).
    """

    def __init__(
        self,
        aioharedis_client: AioHaredisClientCmdExecutor,
        ):
        """Constructor for RedisLockReleaseManager for Redis as Standalone.

        Args:
            aioharedis_client (AioHaredisClientCmdExecutor): AioHaredisClientCmdExecutor Instance.
            redis_logger (logging.Logger): Logger Instance.
        """
             
        self.__aioharedis_client = aioharedis_client
        self.__redis_logger = aioharedis_client.redis_logger
        
    @property
    def aioharedis_client(self):
        return self.__aioharedis_client
        
    @property
    def redis_logger(self):
        return self.__redis_logger
    
    async def _thread_safe_lock_extend(self, lock: Lock, lock_time_extender_add_time: int, lock_time_extender_replace_ttl: bool):
        """Thread safe lock extender for Redis Lock Release Manager."""
        
        _ = await lock.extend(
            additional_time=lock_time_extender_add_time,
            replace_ttl=lock_time_extender_replace_ttl
            ) if await lock.locked() else None
        
        if _ is None:
            return "finish"
        
    async def xdel_event(
        self,
        consumer_stream_key: dict,
        event_id: str,
        event_info: dict,
        delete_event_wait_time: int
        ):
                
        self.redis_logger.debug("Event will be deleted after {delete_event_wait_time} seconds."
                               .format(delete_event_wait_time=delete_event_wait_time))
        await asyncio.sleep(delete_event_wait_time)
                
        delete_resp = await self.aioharedis_client.client_conn.xdel(consumer_stream_key, event_id)
        
        if delete_resp == 1:
            self.redis_logger.debug("Event deleted after produce: {event_info}"
                                   .format(event_info=event_info))
        else:
            self.redis_logger.warning("Event not deleted after produce: {event_info}"
                                      .format(event_info=event_info))
            
    async def lte_warn(
        self,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool, 
        ):
        
        if lock_time_extender_suffix is not None:
            self.redis_logger.warning("lock_time_extender_suffix will be ignored because run_with_lock_time_extender is False.")
        
        if lock_time_extender_add_time is not None:
            self.redis_logger.warning("lock_time_extender_add_time will be ignored because run_with_lock_time_extender is False.")
            
        if lock_time_extender_blocking_time is not None:
            self.redis_logger.warning("lock_time_extender_blocking_time will be ignored because run_with_lock_time_extender is False.")
            
        if lock_time_extender_replace_ttl is not None:
            self.redis_logger.warning("lock_time_extender_replace_ttl will be ignored because run_with_lock_time_extender is False.")
            
    async def run_lte_streams(
        self,
        lock: Lock,
        consumer_stream_key: str,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool
        ):
            
        if lock_time_extender_add_time < 1:
            raise ValueError("lock_time_extender_add_time must be greater than 1. Found: {lock_time_extender_add_time}"
                             .format(lock_time_extender_add_time=lock_time_extender_add_time))
        
        if lock_time_extender_blocking_time < 1:
            raise ValueError("lock_time_extender_blocking_time must be greater than 1.")
        
        if lock_time_extender_blocking_time > lock_time_extender_add_time * 1000:
            raise ValueError("lock_time_extender_blocking_time must be less than lock_time_extender_add_time.")

 
        lock_extend_stream_key = "{consumer_stream_key}.{lock_time_extender_suffix}".format(
            consumer_stream_key=consumer_stream_key,
            lock_time_extender_suffix=lock_time_extender_suffix
            )
        streams = {lock_extend_stream_key: "$"}
        is_locked = await lock.locked()
                
        # While lock is acquired, call lock time extender consumer
        while is_locked:
            consume = await self.aioharedis_client.client_conn.xread(streams=streams, count=1, block=lock_time_extender_blocking_time)
            
            # key, messages = consume[0]
            # last_id, data = messages[0]
            
            # Retrieve data from event
            if len(consume) > 0:
                self.redis_logger.debug("Lock Extender: Event Received from producer: {consume}"
                                        .format(consume=consume))
                key, messages = consume[0]
                last_id, event_data = messages[0]
                data = event_data["result"]
                
                # If data is "end", lock extender will be closed
                if data == "end":
                    self.redis_logger.info("Lock Extender will be close for this stream: {lock_extend_stream_key}"
                                           .format(lock_extend_stream_key=lock_extend_stream_key))
                    _ = await self.aioharedis_client.client_conn.xdel(lock_extend_stream_key, last_id)
                    self.redis_logger.debug("Lock Extender event deleted: {last_id}"
                                            .format(last_id=last_id))
                    
                    await self._thread_safe_lock_extend(lock, lock_time_extender_add_time, lock_time_extender_replace_ttl)
                    return None
                
            # Extend lock expire time
            if lock_time_extender_replace_ttl:
                self.redis_logger.info("Lock expire time will be extended w/ttl: {lock_time_extender_add_time} seconds for {lock_extend_stream_key}"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time, lock_extend_stream_key=lock_extend_stream_key))
            else:
                self.redis_logger.info("Lock expire time will be extended w/expire: {lock_time_extender_add_time} seconds for {lock_extend_stream_key}"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time, lock_extend_stream_key=lock_extend_stream_key))
                
            _ = await self._thread_safe_lock_extend(lock, lock_time_extender_add_time, lock_time_extender_replace_ttl)
              
            if _ == "finish":
                self.redis_logger.error("Redis Lock is released for this stream: {lock_extend_stream_key}"
                                        .format(lock_extend_stream_key=lock_extend_stream_key))
                raise RuntimeError("[FATAL] Redis Lock is released!")
            
    async def aiorun_lte_streams_wrp(
        self,
        aioharedis_client: AioHaredisClientCmdExecutor,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        redis_logger: Union[logging.Logger, None],
        args: tuple,
        **kwargs: dict
        ) -> Any:

        result = await func(*args, **kwargs)
        
        stream_key = "stream:{lock_key}.{lock_time_extender_suffix}".format(lock_key=lock_key, lock_time_extender_suffix=lock_time_extender_suffix)
        end_data = {"result": "end"}
                            
        await aioharedis_client.client_conn.xadd(stream_key, end_data, maxlen=1)
        redis_logger.debug("Lock extender closer event sent from the main function")
        redis_logger.debug("stream_key, end_data:")
        redis_logger.debug("{stream_key} , {end_data}".format(stream_key=stream_key, end_data=end_data))
            
        return result
        
    async def partial_main_selector(
        self,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        is_main_coroutine: bool,
        run_with_lock_time_extender: bool,
        args: tuple,
        kwargs: dict,
    ):

        if run_with_lock_time_extender:
            if is_main_coroutine:
                partial_main = partial(
                    self.aiorun_lte_streams_wrp,
                    self.aioharedis_client,
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    self.redis_logger,
                    args,
                    **kwargs
                    )
                
            else:
                raise NotCoroError("run_with_lock_time_extender is True, but is_main_coroutine is False. It must be True.")
            
        else:
            partial_main = partial(
                func,
                args,
                **kwargs
                )
                
        return partial_main
                    
    async def xconsume_call(
        self,
        lock_key: str,
        streams: dict,
        consumer_blocking_time: int,
        null_handler: Any,
        consumer_stream_key: str,
        consumer_do_retry: bool,
        consumer_retry_count: int,
        consumer_retry_blocking_time_ms: int,
        consumer_max_re_retry: int
        ):
        
        if not isinstance(consumer_blocking_time, int):
            raise TypeError("consumer_blocking_time must be integer.")
        
        if consumer_blocking_time < 0:
            raise ValueError("consumer_blocking_time must be greater than 0.")
                
        self.redis_logger.debug("Lock key: {lock_key} acquire failed. Result will be tried to retrieve from consumer".format(lock_key=lock_key))
        result = await self.aioharedis_client.xconsume(
            streams=streams,
            lock_key=lock_key,
            blocking_time_ms=consumer_blocking_time,
            count=1,
            do_retry=consumer_do_retry,
            retry_count=consumer_retry_count,
            retry_blocking_time_ms=consumer_retry_blocking_time_ms,
            max_re_retry=consumer_max_re_retry
            )
                
        if "from-event" in result.keys():
            
            # Retrieve data from event
            result_from_event = result["from-event"]
            key, messages = result_from_event[0]
            last_id, event_data = messages[0]
            data = event_data["result"]
            event_info = await self.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
            self.redis_logger.info("Event Received from producer: {event_info}".format(event_info=event_info))
            
            if isinstance(data, str) and data == "null":
                # Return null_handler
                return null_handler
            
            if isinstance(data, str) and data.startswith("RedException"):
                
                # Retrive traceback from event if result is RedException
                exc = event_data.get("traceback")
                response = {"traceback": exc, "error": True, "message": data, "from": "haredis consumer", "stream_key": consumer_stream_key}
                return response
            
            data = json.loads(data)
            return data
        
        else:
            raise RuntimeError("An error occured while retrieving data from consumer.")
        
    async def lock_key_generator(
        self,
        keys_to_lock: tuple,
        args: tuple,
        kwargs: dict,
        lock_key_prefix: Union[str, None],
        ) -> str:
        
        if not isinstance(keys_to_lock, tuple):
            raise TypeError("keys_to_lock must be tuple.")
        
        if lock_key_prefix and not isinstance(lock_key_prefix, str):
            raise TypeError("lock_key_prefix must be string or None.")
        
        lock_list = []
        idx = 0
        lock_key_suffix = ".lock"
                
        keys_to_lock_args = [item for item in keys_to_lock if item in args]
        keys_to_lock_kwargs = [item for item in keys_to_lock if item in list(kwargs.keys())]
        self.redis_logger.debug("keys_to_lock_args: {keys_to_lock_args}".format(keys_to_lock_args=keys_to_lock_args))
        self.redis_logger.debug("keys_to_lock_kwargs: {keys_to_lock_kwargs}".format(keys_to_lock_kwargs=keys_to_lock_kwargs))
        
        # Add positional arguments to lock key
        if len(keys_to_lock_args) > 0:
            for idx, lock_name in enumerate(keys_to_lock_args):
                if args.get(lock_name) is None:
                    param = "null"
                else:
                    param = str(args.get(lock_name))
                fmt_str = str(idx+1) + ":" + param
                lock_key = "arg" +  fmt_str
                lock_list.append(lock_key)
        
        # Add keyword arguments to lock key
        if len(keys_to_lock_kwargs) > 0:
            for idx ,lock_name in enumerate(keys_to_lock):
                if kwargs.get(lock_name) is None:
                    param = "null"
                else:
                    param = str(kwargs.get(lock_name))
                fmt_str = str(idx+1) + ":" + param
                lock_key = "param" + fmt_str
                lock_list.append(lock_key)
                
        # If no positional or keyword arguments are provided, raise ValueError
        if len(lock_list) == 0:
            raise ValueError("No lock key parameter is provided.")
        
        if lock_key_prefix:
            lock_key_suffix = ".{lock_key_prefix}{lock_key_suffix}".format(lock_key_prefix=lock_key_prefix, lock_key_suffix=lock_key_suffix)
        
        lock_key = "&".join(lock_list) + lock_key_suffix
        return lock_key
        
    async def run_lte_pubsub(
        self,
        lock: Lock,
        pubsub_channel: str,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool
        ):
            
        if lock_time_extender_add_time < 1:
            raise ValueError("lock_time_extender_add_time must be greater than 1. Found: {lock_time_extender_add_time}"
                             .format(lock_time_extender_add_time=lock_time_extender_add_time))
        
        if lock_time_extender_blocking_time < 1:
            raise ValueError("lock_time_extender_blocking_time must be greater than 1.")
        
        if lock_time_extender_blocking_time > lock_time_extender_add_time * 1000:
            raise ValueError("lock_time_extender_blocking_time must be less than lock_time_extender_add_time.")

        consume = None
 
        lock_extend_pubsub_key = "{pubsub_channel}.{lock_time_extender_suffix}".format(
            pubsub_channel=pubsub_channel,
            lock_time_extender_suffix=lock_time_extender_suffix
            )

        is_locked = await lock.locked()
        
                
        # While lock is acquired, call lock time extender consumer
        while is_locked:
            
            # Subscribe to Channel        
            ps = self.aioharedis_client.client_conn.pubsub()
            await ps.subscribe(lock_extend_pubsub_key)
            
            # Listen to Channel
            async for raw_message in ps.listen():
                if raw_message['type'] == 'message':
                    consume = raw_message['data']
                    try:
                        consume = json.loads(consume)
                        break
                    except Exception as e:
                        self.redis_logger.error("Data is not JSON Encodable!")
                        raise TypeError("Data is not JSON Encodable!") 
                        
            # Retrieve data from event
            if len(consume) > 0:
                self.redis_logger.debug("Lock Extender: Event Received from producer: {consume}"
                                        .format(consume=consume))

                data = consume["result"]
                
                # If data is "end", lock extender will be closed
                if data == "end":
                    self.redis_logger.info("Lock Extender will be close for this stream: {lock_extend_pubsub_key}".format(lock_extend_pubsub_key=lock_extend_pubsub_key))
                    await self._thread_safe_lock_extend(lock, lock_time_extender_add_time, lock_time_extender_replace_ttl)
                    return None
                
            # Extend lock expire time
            if lock_time_extender_replace_ttl:
                self.redis_logger.info("Lock expire time will be extended w/ttl: {lock_time_extender_add_time} seconds for {lock_extend_pubsub_key}"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time, lock_extend_pubsub_key=lock_extend_pubsub_key))
            else:
                self.redis_logger.info("Lock expire time will be extended w/expire: {lock_time_extender_add_time} seconds for {lock_extend_pubsub_key}"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time, lock_extend_pubsub_key=lock_extend_pubsub_key))
                
            _ = await self._thread_safe_lock_extend(lock, lock_time_extender_add_time, lock_time_extender_replace_ttl)
            
            if _ == "finish":
                self.redis_logger.error("Redis Lock is released for this pubsub: {lock_extend_stream_key}"
                                        .format(lock_extend_stream_key=lock_extend_pubsub_key))
                raise RuntimeError("[FATAL] Redis Lock is released!")
            
    async def aiorun_lte_pubsub_wrp(
        self,
        aioharedis_client: AioHaredisClientCmdExecutor,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        redis_logger: Union[logging.Logger, None],
        args: tuple,
        **kwargs: dict
        ) -> Any:
        
        result = await func(*args, **kwargs)
        
        stream_key = "pubsub:{lock_key}.{lock_time_extender_suffix}".format(lock_key=lock_key, lock_time_extender_suffix=lock_time_extender_suffix)
        end_data = {"result": "end"}
                            
        await aioharedis_client.client_conn.publish(channel=stream_key, message=json.dumps(end_data))
        redis_logger.info("Lock extender closer event sent from the main function.")
            
        return result
        
    async def partial_main_selector_pubsub(
        self,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        is_main_coroutine: bool,
        run_with_lock_time_extender: bool,
        args: tuple,
        kwargs: dict,
    ):
                
        if run_with_lock_time_extender: 
            if is_main_coroutine:
                partial_main = partial(
                    self.aiorun_lte_pubsub_wrp,
                    self.aioharedis_client,
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    self.redis_logger,
                    args,
                    **kwargs
                    )
                
            else:
                raise NotCoroError("run_with_lock_time_extender is True, but is_main_coroutine is False. It must be True.")
        else:
            partial_main = partial(
                func,
                args,
                **kwargs
                )
                
        return partial_main
                            
    async def subscriber_call(
        self,
        lock_key: str,
        pubsub_channel: dict,
        consumer_blocking_time: int,
        null_handler: Any,
        consumer_do_retry: bool,
        consumer_retry_count: int,
        consumer_retry_blocking_time_ms: int,
        consumer_max_re_retry: int
        ):
        
        if not isinstance(consumer_blocking_time, int):
            raise TypeError("consumer_blocking_time must be integer.")
        
        if consumer_blocking_time < 0:
            raise ValueError("consumer_blocking_time must be greater than 0.")
                
        self.redis_logger.debug("Lock key: {lock_key} acquire failed. Result will be tried to retrieve from consumer".format(lock_key=lock_key))
        result = await self.aioharedis_client.subscribe_msg(
            pubsub_channel=pubsub_channel,
            lock_key=lock_key,
            do_retry=consumer_do_retry,
            retry_count=consumer_retry_count,
            retry_blocking_time_ms=consumer_retry_blocking_time_ms,
            max_re_retry=consumer_max_re_retry
            )
                
        if result:
            
            # Retrieve data from event
            self.redis_logger.info("Event Received from producer: {event_info}".format(event_info=result))
            
            if isinstance(result, str) and result == "null":
                # Return null_handler
                return null_handler
            
            if isinstance(result, str) and result.startswith("RedException"):
                # Return string as RedException:<exception_string>
                return result
                        
            return json.loads(result)
        
        else:
            raise RuntimeError("An error occured while retrieving data from consumer.")