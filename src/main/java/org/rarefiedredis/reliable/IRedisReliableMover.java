package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public interface IRedisReliableMover<T> {

    String type();

    T verify(IRedisClient client, String source, String dest, T element);

    IRedisClient multi(IRedisClient multi, String source, String dest, T get);
}
