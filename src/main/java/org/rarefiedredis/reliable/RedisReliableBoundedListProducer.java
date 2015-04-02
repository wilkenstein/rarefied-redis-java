package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

import java.util.Map;
import java.util.Collection;

public final class RedisReliableBoundedListProducer implements IRedisReliableProducer<String> {

    private long bound;

    public RedisReliableBoundedListProducer(long bound) {
        this.bound = bound;
    }

    @Override public String type() {
        return "list";
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, String production) {
        try {
            multi.lpush(key, production);
            multi.ltrim(key, -bound, -1L);
        }
        catch (Exception e) {
        }
        return multi;
    }

}