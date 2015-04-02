package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public final class RedisReliableStringMover implements IRedisReliableMover<String> {

    @Override public String type() {
        return "string";
    }

    @Override public String verify(IRedisClient client, String source, String dest, String element) {
        try {
            return client.get(source);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String source, String dest, String get) {
        try {
            multi.del(source);
            multi.set(dest, get);
        }
        catch (Exception e) {
            return null;
        }
        return multi;
    }

}
