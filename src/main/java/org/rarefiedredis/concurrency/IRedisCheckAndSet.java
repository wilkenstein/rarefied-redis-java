package org.rarefiedredis.concurrency;

import org.rarefiedredis.redis.IRedisClient;

import java.util.List;

public interface IRedisCheckAndSet<T> {

    T get(IRedisClient client, String key);
    
    IRedisClient set(IRedisClient multi, String key, T get);

}