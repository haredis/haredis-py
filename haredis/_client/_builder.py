"""Top Level module for AioHaredisClientBuilder class."""

import logging
from typing import Union

import redis

from ._executor import HaredisClientCmdExecutor
from ._interface import AbstractHaredisClientBuilder, AbstractHaredisClientSetter

class HaredisClientBuilder(AbstractHaredisClientBuilder):
    """HaredisClientBuilder class for Redis sync api."""
    
    def __init__(self):
        pass
            
    @property
    def builder(self):
        return HaredisClientSetter()

class HaredisClientSetter(AbstractHaredisClientSetter):
    """Setter class for HaredisClientBuilder class."""
    
    def __init__(self):
        self.__redis_logger = None
        self.__client_conn = None
    
    def set_logger(self, logger: logging.Logger):
        
        if not isinstance(logger, logging.Logger):
            raise TypeError("Logger must be instance of logging.Logger.")
        
        self.__redis_logger = logger
        return self
    
    def set_connection(self, client_conn: Union[redis.StrictRedis, redis.Redis]):
        
        if type(client_conn) not in (redis.StrictRedis, redis.Redis):
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
        return HaredisClientCmdExecutor(self.__client_conn, self.__redis_logger)
    
    @property
    def client_conn(self):
        return self.__client_conn
    
    @property
    def redis_logger(self):
        return self.__redis_logger
    