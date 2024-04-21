"""Haredis AioRedis Client Implementation."""
import abc
import logging
import asyncio
from typing import Union

from redis import asyncio as aioredis
from redis.asyncio.lock import Lock

class AbstractAioHaredisClientSetter(abc.ABC):
    """
    Abstract Setter class for AioHaredisClientBuilder class.
    """
    
            
    @abc.abstractmethod
    def set_logger(self, logger: logging.Logger):
        """Set logger for redis client.

        Args:
            logger (logging.Logger): Logger object.
        """
        
        return NotImplementedError
    
    @abc.abstractmethod
    def set_connection(self, client_conn: Union[aioredis.StrictRedis, aioredis.Redis]):
        """Set redis client connection object.

        Args:
            client_conn (Union[redis.StrictRedis, redis.Redis]): Redis client connection object.
        """
        
        return NotImplementedError
    
    @abc.abstractmethod
    def build(self):
        """Build redis client connection object."""
        
        return NotImplementedError
        
    @property
    @abc.abstractmethod
    def client_conn(self):
        """Getter method for client connection object.

        Returns:
            Union[redis.StrictRedis, redis.Redis]: Redis client connection object.
        """
        
        return NotImplementedError
    
    @property
    @abc.abstractmethod
    def redis_logger(self):
        """Getter method for redis logger.

        Returns:
            logging.Logger: Redis logger.
        """
        
        return NotImplementedError


class AbstractAioHaredisClientBuilder(abc.ABC):
    """
    Abstract haredis client builder class for redis async api.
    """
    
    @property
    @abc.abstractmethod
    def builder(self):
        """Calls builder method for redis client."""
        
        return NotImplementedError

class AbstractAioHaredisClientCmdExecutor(abc.ABC):
    """
    Abstract haredis client class for redis async api.
    """
    

    @property
    @abc.abstractmethod
    def client_conn(self):
        """Getter method for client connection object.

        Returns:
            Union[redis.StrictRedis, redis.Redis]: Redis client connection object.
        """
        
        return NotImplementedError
    
    @property
    @abc.abstractmethod
    def redis_logger(self):
        """Getter method for redis logger.

        Returns:
            logging.Logger: Redis logger.
        """
        
        return NotImplementedError
                
    @abc.abstractmethod
    async def xproduce(self, stream_name: str, data: dict, max_messages: int, maxlen: int, stream_id: str):
        """
        This method produces messages to a Redis Stream. If you want to produce more than 1 event,
        you can use max_messages argument. If you want to produce events infinitely, you can set max_messages to None.

        Args:
            stream_name (string): Name of the stream.
            data (dict): Data to be sent to the stream as event.
            max_messages (int): Max message limit. If None, it will produce messages infinitely.
            stream_id (str): Stream id. If *, it will generate unique id automatically.

        Raises:
            RuntimeError: If producer name is not provided when strict is true.
            
        """
        
        return NotImplementedError
    
    @abc.abstractmethod
    async def xconsume(
        self,
        streams: dict,
        lock_key: str,
        blocking_time_ms: int,
        count: int,
        do_retry: bool,
        retry_count: int,
        retry_blocking_time_ms: int,
        max_re_retry: int
        
        ):
        """This method consumes messages from a Redis Stream infinitly w/out consumer group.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            lock_key (str): Name of the lock key.
            blocking_time_ms (int): Blocking time in miliseconds.
            count (int): if set, only return this many items, beginning with the
               earliest available.
            do_retry (bool): If True, it will retry to acquire lock.
            retry_count (int): Retry count.
            retry_blocking_time_ms (int): Retry blocking time in miliseconds
            max_re_retry (int): Max re-retry count for lock release problem
               
        Returns:
            Any: Returns consumed events as list of dicts.

        """
        
        return NotImplementedError
        
    @abc.abstractmethod     
    async def create_consumer_group(self, stream_name: str, group_name: str, mkstream: bool, id: str):
        """
        This method allows to creates a consumer group for a stream.
        You can create multiple consumer groups for a stream.
        Also best practice is to create consumer group before producing events to stream.
        You can create consumer group with mkstream=True to create stream if not exists.
        In FastAPI, you can create consumer groups in startup event.

        Args:
            stream_name (string): Name of the stream. 
            id (string): Stream id. If "$", it will try to get last id of the stream.
            group_name (string): Name of the consumer group.
        """
                
        return NotImplementedError

    @abc.abstractmethod
    async def xconsumegroup(
        self,
        streams: dict,
        group_name: str,
        consumer_name: str,
        blocking_time_ms: int,
        count: int,
        noack: bool,
        ):
        """
        This method consumes messages from a Redis Stream infinitly as consumer group.
        If you want to consume events as consumer group, you must create consumer group first.
        Also you should delete events from stream after consuming them. Otherwise memory will be full in redis.
        You can delete events from stream with xtrim or xdel commands.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            group_name (str): Name of the consumer group.
            consumer_name (str): Name of the requesting consumer.
            blocking_time_ms (int): Blocking time.
            count (int): if set, only return this many items, beginning with the
               earliest available.
            noack (bool): If True, it will not add events to pending list.

        Returns:
            Any: Returns consumed events as list of dicts.
        """
        
        return NotImplementedError
            
    @abc.abstractmethod
    async def get_last_stream_id(self, stream_name: str):
        """Get Last Stream Id from stream."""
        
        return NotImplementedError
        
    @abc.abstractmethod                   
    async def xtrim_with_id(self, stream_name: str, id: str):
        """This method allows to delete events from stream w/ trim.
        After deleting events from stream, you can not consume them again.
        For this reason, you should use this method carefully.
        This function provides to free memory for redis.

        Args:
            stream_name (str): Name of the stream.
            id (str): Name of the event id.

        Returns:
            Any: Deletion response or Warning message if stream not exists.
        """
                
        return NotImplementedError
               
    @abc.abstractmethod     
    async def publish_msg(self, pubsub_channel: str, message: str):
        """
        Send events to pub-sub channel.

        Args:
            pubsub_channel (str): Pub-Sub channel name which will be used to publish events
            message (str): Event to publish to subscribers channel

        """
 
        return NotImplementedError

    @abc.abstractmethod
    async def subscribe_msg(
        self,
        pubsub_channel: str,
        lock_key: str,
        do_retry: bool,
        retry_count: int,
        retry_blocking_time_ms: int,
        max_re_retry: int
        ):
        """
        Firstly it subscribes to pubsub_channel and then when it receives an event, it consumes it.

        Args:
            pubsub_channel (str): Name of the pub-sub channel to subscribe which was created by publisher
            lock_key (str): Name of the lock key.
            do_retry (bool): If True, it will retry to acquire lock.
            retry_count (int): Retry count.
            retry_blocking_time_ms (int): Retry blocking time in miliseconds.
            max_re_retry (int): Max re-retry count for lock release problem

        Returns:
            Event: Event from publisher.
        """
        
        return NotImplementedError
        
    @abc.abstractmethod
    async def acquire_lock(self, lock_key: str, expire_time: int):
        """This function allows to assign a lock to a key for distrubuted caching.

        Args:
            lock_key (str): Name of the Lock key.
            expire_time (int): Expire time for lock key. It is in seconds.

        Returns:
            Lock: Redis Lock object.
        """
        
        return NotImplementedError
    
    @abc.abstractmethod
    async def is_locked(self, redis_lock: Lock):
        """Check if lock object is locked or not."""
                
        return NotImplementedError
    
    @abc.abstractmethod
    async def is_owned(self, lock: Lock):
        """Check if lock object is owned or not."""
        
        return NotImplementedError
    
    @abc.abstractmethod
    async def release_lock(self, redis_lock: Lock):
        """Try to release lock object."""
        
        return NotImplementedError
        
    @abc.abstractmethod    
    async def is_aioredis_available(self):
        """Helper method to check if aioredis is available or not."""
        
        return NotImplementedError