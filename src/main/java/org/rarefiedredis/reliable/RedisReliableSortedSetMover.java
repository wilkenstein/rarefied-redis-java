package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.IRedisSortedSet.ZsetPair;

import java.util.Set;

public final class RedisReliableSortedSetMover implements IRedisReliableMover<String> {

    private Double score;

    @Override public String type() {
        return "zset";
    }

    @Override public String verify(IRedisClient client, String source, String dest, String element) {
        try {
            if (element == null) {
                Set<ZsetPair> range = client.zrange(source, 0L, 0L);
                if (range.isEmpty()) {
                    return null;
                }
                element = range.iterator().next().member;
            }
            score = client.zscore(source, element);
            return (score != null ? element : null);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String source, String dest, String get) {
        try {
            multi.zrem(source, get);
            multi.zadd(dest, new ZsetPair(get, score));
        }
        catch (Exception e) {
            return null;
        }
        return multi;
    }

}
