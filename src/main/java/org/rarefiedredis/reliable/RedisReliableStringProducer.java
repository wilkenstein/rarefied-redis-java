package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

import java.util.Map;
import java.util.Collection;

public final class RedisReliableStringProducer implements IRedisReliableProducer<String> {

    @Override public String type() {
        return "string";
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, String production) {
        try {
            multi.set(key, production);
        }
        catch (Exception e) {
        }
        return multi;
    }

}