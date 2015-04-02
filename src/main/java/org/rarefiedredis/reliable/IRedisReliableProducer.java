package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

import java.util.Collection;

public interface IRedisReliableProducer<T> {

    String type();

    IRedisClient multi(IRedisClient multi, String key, T production);

}