package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public interface IRedisReliableDeleter<T> {

    String type();

    T verify(IRedisClient client, String key, T element);

    IRedisClient multi(IRedisClient multi, String key, T element);
}
