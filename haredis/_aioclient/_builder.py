"""Top Level module for AioHaredisClientBuilder class."""

import logging
from typing import Union

from redis import asyncio as aioredis

from ._executor import AioHaredisClientCmdExecutor
from ._interface import AbstractAioHaredisClientBuilder, AbstractAioHaredisClientSetter

class AioHaredisClientBuilder(AbstractAioHaredisClientBuilder):
    """AioHaredisClientBuilder class for Redis async api."""
    
    def __init__(self):
        pass
            
    @property
    def builder(self):
        return AioHaredisClientSetter()

class AioHaredisClientSetter(AbstractAioHaredisClientSetter):
    """Setter class for AioHaredisClientBuilder class."""
    
    def __init__(self):
        self.__redis_logger = None
        self.__client_conn = None
    
    def set_logger(self, logger: logging.Logger):
        
        if not isinstance(logger, logging.Logger):
            raise TypeError("Logger must be instance of logging.Logger.")
        
        self.__redis_logger = logger
        return self
    
    def set_connection(self, client_conn: Union[aioredis.StrictRedis, aioredis.Redis]):
        
        if type(client_conn) not in (aioredis.StrictRedis, aioredis.Redis):
            raise TypeError("Client connection must be redis.StrictRedis or redis.Redis")
        
        self.__client_conn = client_conn
        return self
    
    def build(self):
        
        if not self.__redis_logger:
            self.__redis_logger = logging.getLogger('dummy')
            self.__redis_logger.setLevel(logging.CRITICAL)
            self.__redis_logger.addHandler(logging.NullHandler())
            
        
        if not self.__client_conn:
            raise Exception("Client connection is not set for Redis Client.")
        
        self.__redis_logger.debug("AioHaredisClientCmdExecutor built successfully.")
        return AioHaredisClientCmdExecutor(self.__client_conn, self.__redis_logger)
    
    @property
    def client_conn(self):
        return self.__client_conn
    
    @property
    def redis_logger(self):
        return self.__redis_logger
    