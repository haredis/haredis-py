"""Implementation of Distributed Lock-Release Algorithm for Distributed Caching Locking in Redis"""

from typing import Callable, Union, Any
import logging
import abc

from redis.asyncio.lock import Lock

from .._aioclient import AioHaredisClientCmdExecutor 


class AbstractBaseAioHaredisManager(abc.ABC):
    """## Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using streams and pub/sub api (For asyncronous execution).
    """
        
    @property
    @abc.abstractmethod
    def aioharedis_client(self):
        """Getter for aioharedis_client"""
        
        raise NotImplementedError
        
    @property
    @abc.abstractmethod
    def redis_logger(self):
        """Getter for redis_logger"""
        
        raise NotImplementedError
    
    @abc.abstractmethod 
    async def xdel_event(
        self,
        consumer_stream_key: dict,
        event_id: str,
        event_info: dict,
        delete_event_wait_time: int
        ):
        """Delete event after provided time seconds for clean up for data consistency and memory usage
        
        Args:
            consumer_stream_key (dict): Name of the stream key
            lock_key (str): Name of the lock key
            event_id (str): Id of the event
            event_info (dict): Info of the event
            delete_event_wait_time (int): Wait time for delete event operation in seconds.
        """
                
        raise NotImplementedError
    
    @abc.abstractmethod
    async def lte_warn(
        self,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool, 
        ):
        """Warn for lock time extender.
        
        Args:
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed in seconds.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. If False, expire will be used instead of ttl.
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod    
    async def run_lte_streams(
        self,
        lock: Lock,
        consumer_stream_key: str,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool
        ):
        """Asyncronous lock time extender for lock release manager
        
        Args:
            lock (Lock): Lock object
            consumer_stream_key (str): Name of the stream key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed in seconds.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. If False, expire will be used instead of ttl.
        
        """
            
        raise NotImplementedError
      
    @abc.abstractmethod        
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
        """Execute asyncronous function with close lock extender for finish lock extender after async main function execution.

        Args:
            aioharedis_client (AioHaredisClientCmdExecutor): AioHaredisClientCmdExecutor Instance
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            redis_logger (Union[logging.Logger, None], optional): Logger Instance.
            
        Returns:
            Any: Result of the function
        """
        
        raise NotImplementedError
       
    @abc.abstractmethod   
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
        """Select partial main function based on is_main_coroutine parameter

        Args:
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key.
            lock_time_extender_suffix (str): Suffix for lock extender stream key.
            is_main_coroutine (bool): If True, execute asyncronous function, if False, execute syncronous function.
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability.
            args (tuple): args
            kwargs (dict): kwargs
            
        Returns:
            Partial main function.
        """
        
        raise NotImplementedError
     
    @abc.abstractmethod                 
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
        """Call consumer when lock is not owned by current process and if already acquired by another process

        Args:
            lock_key (str): Name of the lock key.
            streams (dict): Streams to be consumed.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null).
            consumer_stream_key (str): Name of the stream key.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released.
            consumer_retry_count (int): Retry count for consumer.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry.
            consumer_max_re_retry (int): Max re-retry count for consumer.

        Returns:
            Any: Result of the function
        """
        
        raise NotImplementedError
      
    @abc.abstractmethod    
    async def lock_key_generator(
        self,
        keys_to_lock: tuple,
        args: tuple,
        kwargs: dict,
        lock_key_prefix: Union[str, None],
        ) -> str:
        """This function generates lock key based on positional and keyword arguments

        Args:
            keys_to_lock (tuple): Keys to be locked
            args (tuple): function positional arguments
            kwargs (dict): function keyword arguments
            lock_key_prefix (Union[str, None], optional): Prefix for lock key.

        Raises:
            ValueError: If positional or keyword arguments are missing, raise ValueError

        Returns:
            str: lock key
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod                  
    async def run_lte_pubsub(
        self,
        lock: Lock,
        pubsub_channel: str,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time: int,
        lock_time_extender_blocking_time: int,
        lock_time_extender_replace_ttl: bool
        ):
        """Asyncronous lock time extender for lock release manager w/pubsub
        
        Args:
            lock (Lock): Lock object
            pubsub_channel (str): Name of the pub/sub channel
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed in seconds.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. If False, expire will be used instead of ttl.
        
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod          
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
        """Execute asyncronous function with close lock extender for finish lock extender after async main function execution.

        Args:
            aioharedis_client (AioHaredisClientCmdExecutor): AioHaredisClientCmdExecutor Instance
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            redis_logger (Union[logging.Logger, None], optional): Logger Instance.
            
        Returns:
            Any: Result of the function
        """
        
        raise NotImplementedError
      
    @abc.abstractmethod    
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
        """Select partial main function based on is_main_coroutine parameter w/pubsub

        Args:
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key.
            lock_time_extender_suffix (str): Suffix for lock extender stream key.
            is_main_coroutine (bool): If True, execute asyncronous function, if False, execute syncronous function.
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability.
            args (tuple): args
            kwargs (dict): kwargs
            
        Returns:
            Partial main function.
        """
                
        raise NotImplementedError
         
    @abc.abstractmethod                   
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
        """Call consumer when lock is not owned by current process and if already acquired by another process w/pubsub

        Args:
            lock_key (str): Name of the lock key.
            pubsub_channel (str): Name of the pub/sub channel.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null).
            consumer_do_retry (bool): If True, consumer will be retried, if lock released.
            consumer_retry_count (int): Retry count for consumer.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry.
            consumer_max_re_retry (int): Max re-retry count for consumer.

        Returns:
            Any: Result of the function
        """
        
        raise NotImplementedError
    
class AbstractAioLockReleaseManager(abc.ABC):
    """## Aio Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using streams and pub/sub api (For asyncronous execution).
    """

    @property
    @abc.abstractmethod
    def rl_manager(self):
        
        raise NotImplementedError
        
    @abc.abstractmethod
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
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        args=tuple(),
        **kwargs: dict
        ):
        """haredis distributed locking algorithm implementation in redis using stream api xread/xadd (For both syncronous/asyncronous execution).

        Args:
            func (Callable): Function name to be executed.
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            delete_event_wait_time (int): Wait time for delete event operation. Defaults to 10.
            redis_availability_strategy (str): Redis availabilty strategy. Defaults to "error". If "error", raise exception
                if redis is not available, if "continue", continue execution of function without redis if redis is not available.
            args (tuple): Arguments for the function. Defaults to tuple().
            kwargs (dict): Keyword arguments for the function. Defaults to {}.

        Returns:
            Any: Result of the function
        """
        
        raise NotImplementedError
    
    @abc.abstractmethod
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
        """haredis distributed locking algorithm implementation in redis using pub/sub api (For both syncronous/asyncronous execution).

        Args:
            func (Callable): Function name to be executed.
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            redis_availability_strategy (str): Redis availabilty strategy. Defaults to "error". If "error", raise exception
                if redis is not available, if "continue", continue execution of function without redis if redis is not available.
            response_cache (int, optional): If provided, cache response with provided time in seconds. Defaults to None.
            extend_cache_time (bool, optional): If True, extend cache time with response cache parameter. Defaults to False.
            args (tuple): Arguments for the function. Defaults to tuple().
            kwargs (dict): Keyword arguments for the function. Defaults to {}.

        Returns:
            Any: Result of the function
        """
    
    @abc.abstractmethod                          
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
        """haredis distributed locking algorithm implementation in redis using stream api xread/xadd (For both syncronous/asyncronous execution) as decorator.

        Args:
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            delete_event_wait_time (int): Wait time for delete event operation. Defaults to 10.
            redis_availability_strategy (str): Redis availabilty strategy. Defaults to "error". If "error", raise exception
                if redis is not available, if "continue", continue execution of function without redis if redis is not available.

        Returns:
            Any: Result of the function
        """

        raise NotImplementedError
    
    @abc.abstractmethod
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
        """haredis distributed locking algorithm implementation in redis using pub/sub api (For both syncronous/asyncronous execution) as decorator.

        Args:
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            delete_event_wait_time (int): Wait time for delete event operation. Defaults to 10.
            redis_availability_strategy (str): Redis availabilty strategy. Defaults to "error". If "error", raise exception
                if redis is not available, if "continue", continue execution of function without redis if redis is not available.
            response_cache (int, optional): If provided, cache response with provided time in seconds. Defaults to None.
            extend_cache_time (bool, optional): If True, extend cache time with response cache parameter. Defaults to False.

        Returns:
            Any: Result of the function
        """

        raise NotImplementedError