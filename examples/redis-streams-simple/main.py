from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import time
from haredis import LockReleaseManager, HaredisClientBuilder, AioLockReleaseManager, AioHaredisClientBuilder, set_up_logging
import redis
from redis import asyncio as aioredis


# Create Redis Clients
_REDIS = redis.Redis(
    host="0.0.0.0",
    port=6379,
    db=0,
    password="examplepass",
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)   

_AIOREDIS = aioredis.Redis(
    host="0.0.0.0",
    port=6379,
    db=0,
    password="examplepass",
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)

# Set up logging
_logger = set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")

# Create Haredis Clients
haredis_client = HaredisClientBuilder().builder \
                    .set_connection(_REDIS) \
                    .set_logger(_logger) \
                    .build()
              
aioharedis_client = AioHaredisClientBuilder().builder \
                    .set_connection(_AIOREDIS) \
                    .set_logger(_logger) \
                    .build()

# Create Lock Release Managers
RL_MANAGER = LockReleaseManager(
    haredis_client,
    )    
     
AIORL_MANAGER = AioLockReleaseManager(
    aioharedis_client,
    )


# Create a connection pools
async def create_aioredis_pool() -> aioredis.ConnectionPool:
    return aioredis.ConnectionPool(
        host="0.0.0.0",
        port=6379,
        db=0,
        password="examplepass",
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )
    
def create_redis_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(
        host="0.0.0.0",
        port=6379,
        db=0,
        password="examplepass",
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )

# Get FastAPI Application
def get_application() -> FastAPI:
    _app = FastAPI()

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return _app
    
app = get_application()

# Configure startup - shutdown events
@app.on_event("startup")
async def startup_event():
        
    # Create Redis Connection Pools
    redis_pool = create_redis_pool()
    aioredis_pool = await create_aioredis_pool()
    
    # Create Redis Clients
    app.state.redis = redis.Redis(connection_pool=redis_pool)
    app.state.aioredis = aioredis.Redis(connection_pool=aioredis_pool)
        
    # Check Redis Connections
    ping = app.state.redis.ping()
    if not ping:
        raise Exception("Redis Connection Error!")
    print("Redis Connection OK!")
    
    ping = await app.state.aioredis.ping()
    if not ping:
        raise Exception("AioRedis Connection Error!")
    print("AioRedis Connection OK!")
                    

@app.on_event("shutdown")
async def shutdown_event():
    
    # Close Redis Clients    
    await app.state.aioredis.close()
    app.state.redis.close()
    
## Functions without lock release, which will be used in lock release functions on examples
def example_function(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
        
    time.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

async def example_async_function(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
    
    print("Main Async function is running...")
    
    await asyncio.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result


# Section: AIORL_MANAGER For both sync and async functions
# --------------------------------------------------------

# Option 1: Define functions to be used in routes (if you want to use lock release decorator)
@AIORL_MANAGER.lock_release_decorator_streams(
    keys_to_lock=("param1", "param2"),
    lock_key_prefix=None,
    lock_expire_time=30,
    consumer_blocking_time=1000 * 2,
    consumer_do_retry=True,
    consumer_retry_count=5,
    consumer_retry_blocking_time_ms=2 * 1000,
    consumer_max_re_retry=2,
    null_handler={},
    run_with_lock_time_extender=True,
    lock_time_extender_suffix="lock_extender",
    lock_time_extender_add_time=10,
    lock_time_extender_blocking_time=5 * 1000,
    lock_time_extender_replace_ttl=True,
    delete_event_wait_time=10,
    redis_availability_strategy="error",
)
def sync_decorated_style(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
        
    time.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

@AIORL_MANAGER.lock_release_decorator_streams(
    keys_to_lock=("param1", "param2"),
    lock_key_prefix=None,
    lock_expire_time=30,
    consumer_blocking_time=1000 * 2,
    consumer_do_retry=True,
    consumer_retry_count=5,
    consumer_retry_blocking_time_ms=2 * 1000,
    consumer_max_re_retry=2,
    null_handler={},
    run_with_lock_time_extender=True,
    lock_time_extender_suffix="lock_extender",
    lock_time_extender_add_time=10,
    lock_time_extender_blocking_time=5 * 1000,
    lock_time_extender_replace_ttl=True,
    delete_event_wait_time=10,
    redis_availability_strategy="error",
)
async def async_decorated_style(*args, **kwargs):
    """Async Example Function without lock release."""
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
    
    print("Main Async function is running...")
    
    await asyncio.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

# Option 2: Define functions to be used in routes (if you do not want to use lock release decorator)
## Functions with lock release which they wrap functions without lock release on top
async def async_event_rl_native(*args, **kwargs):
    
    result = await AIORL_MANAGER.lock_release_with_stream(
        func=example_async_function,
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        args=(),
        **kwargs
    )
               
    return result

async def sync_event_rl_native(*args, **kwargs):
                                            
    result = await AIORL_MANAGER.lock_release_with_stream(
        func=example_function,
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        args=(),
        **kwargs
    )
            
    return result

# Section: RL_MANAGER For only sync functions
# --------------------------------------------------------

@RL_MANAGER.lock_release_decorator_streams(
    keys_to_lock=("param1", "param2"),
    lock_key_prefix=None,
    lock_expire_time=30,
    consumer_blocking_time=1000 * 2,
    consumer_do_retry=True,
    consumer_retry_count=5,
    consumer_retry_blocking_time_ms=2 * 1000,
    consumer_max_re_retry=2,
    null_handler={},
    run_with_lock_time_extender=True,
    lock_time_extender_suffix="lock_extender",
    lock_time_extender_add_time=10,
    lock_time_extender_blocking_time=5 * 1000,
    lock_time_extender_replace_ttl=True,
    delete_event_wait_time=10,
    redis_availability_strategy="error",
)
def sync_decorated_style2(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
        
    time.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

def sync_event_rl_native2(*args, **kwargs):
                                            
    result = RL_MANAGER.lock_release_with_stream(
        func=example_function,
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        args=(),
        **kwargs
    )
            
    return result

# Define routes for decorated style for AIORL_MANAGER
# --------------------------------------------------------

# Define routes for decorated style
@app.get("/sync_decorated_style")
async def sync_decorated_style_route(param1: int, param2: int):
    """Sync Example Function with lock release with AIORL_MANAGER."""
    
    result = await sync_decorated_style(param1=param1, param2=param2)
    
    return result

@app.get("/async_decorated_style")
async def async_decorated_style_route(param1: int, param2: int):
    """Async Example Function with lock release with AIORL_MANAGER."""
    
    result = await async_decorated_style(param1=param1, param2=param2)
    
    return result


# Define routes for non-decorated style
@app.get("/sync_non_decorated_style")
async def sync_non_decorated_style_route(param1: int, param2: int):
    """Sync Example Function without lock release with AIORL_MANAGER."""
    
    result = await sync_event_rl_native(param1=param1, param2=param2)
    
    return result

@app.get("/async_non_decorated_style")
async def async_non_decorated_style_route(param1: int, param2: int):
    """Async Example Function without lock release with AIORL_MANAGER."""
    
    result = await async_event_rl_native(param1=param1, param2=param2)
    
    return result



# Define routes for decorated style for RL_MANAGER
# --------------------------------------------------------

# Define routes for decorated style
@app.get("/sync_decorated_style2")
def sync_decorated_style_route2(param1: int, param2: int):
    """Sync Example Function with lock release with RL_MANAGER."""
    
    result = sync_decorated_style2(param1=param1, param2=param2)
    
    return result

# Define routes for non-decorated style
@app.get("/sync_non_decorated_style2")
def sync_non_decorated_style_route2(param1: int, param2: int):
    """Sync Example Function without lock release with RL_MANAGER."""
    
    result = sync_event_rl_native2(param1=param1, param2=param2)
    
    return result

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )