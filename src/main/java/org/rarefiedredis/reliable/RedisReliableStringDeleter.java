package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public final class RedisReliableStringDeleter implements IRedisReliableDeleter<String> {

    @Override public String type() {
        return "string";
    }

    @Override public String verify(IRedisClient client, String key, String element) { 
        try {
            return client.get(key);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, String element) {
        try {
            multi.del(key);
        }
        catch (Exception e) {
        }
        return multi;
    }
}
