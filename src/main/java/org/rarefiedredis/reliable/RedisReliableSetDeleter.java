package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public final class RedisReliableSetDeleter implements IRedisReliableDeleter<String> {

    @Override public String type() {
        return "set";
    }

    @Override public String verify(IRedisClient client, String key, String element) { 
        try {
            return (client.sismember(key, element) ? element : null);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, String element) {
        try {
            multi.srem(key, element);
        }
        catch (Exception e) {
        }
        return multi;
    }
}
