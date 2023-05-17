import os

import redis

USERNAME = os.environ.get('REDISOLAR_REDIS_USERNAME')
PASSWORD = os.environ.get('REDISOLAR_REDIS_PASSWORD')


def get_redis_connection(hostname, port, username=USERNAME, password=PASSWORD):
    client_kwargs = {
        "host": hostname,
        "port": port,
        "decode_responses": True
    }
    if password:
        client_kwargs["password"] = password
    if username:
        client_kwargs["username"] = username

    return redis.Redis(**client_kwargs)


def get_redis_timeseries_connection(hostname, port, username=USERNAME, password=PASSWORD):
    return get_redis_connection(hostname, port, username, password)
