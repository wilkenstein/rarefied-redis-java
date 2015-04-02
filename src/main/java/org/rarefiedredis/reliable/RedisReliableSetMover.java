package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

public final class RedisReliableSetMover implements IRedisReliableMover<String> {

    @Override public String type() {
        return "set";
    }

    @Override public String verify(IRedisClient client, String source, String dest, String element) {
        try {
            if (element == null) {
                element = client.srandmember(source);
            }
            return (client.sismember(source, element) ? element : null);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String source, String dest, String get) {
        try {
            multi.smove(source, dest, get);
        }
        catch (Exception e) {
            return null;
        }
        return multi;
    }

}
