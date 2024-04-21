import logging
import time
from typing import Union

import redis
from redis.lock import Lock

from ._interface import AbstractHaredisClientCmdExecutor
from ._exceptions import LockReleasedError, MaxRetriesExceededOnXReadError

class HaredisClientCmdExecutor(AbstractHaredisClientCmdExecutor):
    """
    haredis client class for redis sync api.
    """
    
    def __init__(self, client_conn: Union[redis.StrictRedis, redis.Redis], redis_logger: logging.Logger):
        
        self.__client_conn = client_conn
        self.__redis_logger = redis_logger
                    
    @property
    def client_conn(self):
        
        return self.__client_conn
    
    @property
    def redis_logger(self):
        
        return self.__redis_logger
                
    def xproduce(
        self,
        stream_name: str,
        data: dict,
        max_messages: int,
        maxlen: int,
        stream_id: str
        ):
        
        # Counter for max message limit
        count = 0
                        
        # Add event to stream as limited message (Defaults to 1 message to add stream).
        if max_messages:
            while count < max_messages:
                _ = self.client_conn.xadd(name=stream_name, fields=data, id=stream_id, maxlen=maxlen)

                # info = await self.client_conn.xinfo_stream(stream_name)
                # print(f"Produced Event Info: {info}")
                time.sleep(1)
                
                # increase count for max message limit
                count += 1 
                if count == max_messages:
                    break
        
        # Add event to stream infinitely.
        else:
            self.redis_logger.warning("Events will be produced infinitely. For stop producing, kill the process.")
            while True:
                _ = self.client_conn.xadd(name=stream_name, fields=data, id=stream_id)
                
                # Write event info to console.
                # info = await self.client_conn.xinfo_stream(stream_name)
                # print(f"Event Info: {info}")
                
                time.sleep(1)
            
    def xconsume(
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
        
        re_retry_counter = 0
        
        while True:
            
            resp = self.client_conn.xread(
                streams=streams,
                count=count,
                block=blocking_time_ms
            )
        
            if resp:
                key, messages = resp[0]
                # last_id, data = messages[0]           
                return {"from-event": resp}
            
            lock_key_status = self.client_conn.get(lock_key)
                        
            if lock_key_status is None:
                
                # If max_retry is reached, raise exception
                if re_retry_counter == max_re_retry:
                    raise MaxRetriesExceededOnXReadError("Max re-retry count reached for Consumer.")
                
                # If not do_retry when lock released, raise exception directly
                if not do_retry:
                    raise LockReleasedError("Redis Lock is released! Consumer can't be consume events from stream.")
                
                self.redis_logger.warning("""Lock is not acquired or released! Consumer will retry consume events from stream again in {retry_count}
                    times with {retry_blocking_time_ms} seconds blocking..""".format(retry_count=retry_count, retry_blocking_time_ms=retry_blocking_time_ms))
                
                # Wait for retry_blocking_time_ms seconds
                for i in range(retry_count+1):
                    i_plus_one = i + 1
                    if i == retry_count:
                        raise MaxRetriesExceededOnXReadError("Max retry count reached for Consumer.")
                    
                    self.redis_logger.info("Retrying to get redis lock: {i_plus_one}.time".format(i_plus_one=i_plus_one))
                    
                    # Wait for retry_blocking_time_ms seconds
                    time.sleep(retry_blocking_time_ms/1000)
                    
                    # Query lock key status again for retry, if lock key is reacquired, break the loop
                    # And go to the beginning of the while loop after incrementing retry_counter
                    lock_key_status = self.client_conn.get(lock_key)
                    if lock_key_status:

                        self.redis_logger.info("New redis lock obtained after retrying {i_plus_one} time. Consumer will be retry to consume events from stream.".format(i_plus_one=i_plus_one))
                        re_retry_counter = re_retry_counter + 1
                        break
             
    def create_consumer_group(self, stream_name: str, group_name: str, mkstream=False, id="$"):

        try:
            resp = self.client_conn.xgroup_create(name=stream_name, id=id, groupname=group_name, mkstream=mkstream)
            self.redis_logger.info("Consumer group created Status: {resp}".format(resp=resp))
        except Exception as e:
            self.redis_logger.warning("Consumer group already exists. {e}".format(e=e))
            info = self.client_conn.xinfo_groups(stream_name)
            self.redis_logger.info("Consumer group info: {info}".format(info=info))

    async def xconsumegroup(
        self,
        streams: dict,
        group_name: str,
        consumer_name: str,
        blocking_time_ms: int,
        count: int,
        noack: bool,
        ):
        
        while True:
            resp = await self.client_conn.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams=streams,
                block=blocking_time_ms,
                count=count,
                noack=noack
                )
                        
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                # print(f"Event id: {last_id}")
                # print(f"Event data {data}")
                return resp
            
    def get_last_stream_id(self, stream_name: str):
        
        last_id = self.client_conn.xrevrange(stream_name, count=1)
        return last_id[0][0]
                           
    def xtrim_with_id(self, stream_name: str, id: str):
                
        is_stream_exists = self.client_conn.exists(stream_name)
        if not is_stream_exists:
            resp = self.client_conn.execute_command('XTRIM', stream_name, 'MINID', id)
            return resp
        return "WARNING: Stream does not exists..."
                    
    async def publish_msg(self, pubsub_channel: str, message: str):
 
        # Send Serialized Event to channel
        event = await self.client_conn.publish(channel=pubsub_channel, message=message)
        return event

    def subscribe_msg(
        self,
        pubsub_channel: str,
        lock_key: str,
        do_retry = True,
        retry_count = 5,
        retry_blocking_time_ms = 2 * 1000,
        max_re_retry = 2
        ):
        
        re_retry_counter = 0
        
        # Subscribe to Channel        
        ps = self.client_conn.pubsub()
        ps.subscribe(pubsub_channel)
        
        # Listen to Channel
        for raw_message in ps.listen():
            if raw_message['type'] == 'message':
                result = raw_message['data']
                # try:
                #     result = json.loads(result)
                # except Exception as e:
                #     self.redis_logger.error("Data is not JSON Encodable!")
                #     raise TypeError("Data is not JSON Encodable!") 
                return result
                
            lock_key_status = self.client_conn.get(lock_key)
                        
            if lock_key_status is None:
                
                # If max_retry is reached, raise exception
                if re_retry_counter == max_re_retry:
                    raise MaxRetriesExceededOnXReadError("Max re-retry count reached for Consumer.")
                
                # If not do_retry when lock released, raise exception directly
                if not do_retry:
                    raise LockReleasedError("Redis Lock is released! Consumer can't be consume events from stream.")
                
                self.redis_logger.warning("Lock is not acquired or released! Consumer will retry consume events from stream again in {retry_count} \
                    times with {retry_blocking_time_ms} seconds blocking..".format(retry_count=retry_count, retry_blocking_time_ms=retry_blocking_time_ms))
                
                # Wait for retry_blocking_time_ms seconds
                for i in range(retry_count+1):
                    i_plus_one = i + 1
                    if i == retry_count:
                        raise MaxRetriesExceededOnXReadError("Max retry count reached for Consumer.")
                    
                    self.redis_logger.info("Retrying to get redis lock: {i_plus_one}.time".format(i_plus_one=i_plus_one))
                    
                    # Wait for retry_blocking_time_ms seconds
                    time.sleep(retry_blocking_time_ms/1000)
                    
                    # Query lock key status again for retry, if lock key is reacquired, break the loop
                    # And go to the beginning of the while loop after incrementing retry_counter
                    lock_key_status = self.client_conn.get(lock_key)
                    if lock_key_status:

                        self.redis_logger.info("New redis lock obtained after retrying {i_plus_one} time. Consumer will be retry to consume events from stream.".format(i_plus_one=i_plus_one))
                        re_retry_counter = re_retry_counter + 1
                        break
        
    def acquire_lock(self, lock_key: str, expire_time: int):
        
        lock = self.client_conn.lock(name=lock_key, timeout=expire_time, blocking_timeout=5, thread_local=False)
        is_acquired = lock.acquire(blocking=True, blocking_timeout=0.01)
        return lock
    
    def is_locked(self, redis_lock: Lock):
                
        if redis_lock.locked():
            return True
        return False
    
    def is_owned(self, redis_lock: Lock):
        
        if redis_lock.owned():
            return True
        return False
    
    def release_lock(self, redis_lock: Lock):
        
        if self.client_conn.get(redis_lock.name) is not None:
            _ = redis_lock.release()
        else:
            self.redis_logger.warning("Redis Lock does not exists! Possibly it is expired. Increase expire time for Lock.")
            
    def is_redis_available(self):
        
        try:
            self.client_conn.ping()
            self.redis_logger.debug("Successfully connected to Redis Server!")
        except Exception as e:
            message = "AioRedis Connection Error! Error: {e}".format(e=e)
            self.redis_logger.error(message)
            return False, message
        return True, None