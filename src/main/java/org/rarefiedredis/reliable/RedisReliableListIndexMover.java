package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;

import java.util.Map;
import java.security.SecureRandom;
import java.math.BigInteger;

public final class RedisReliableListIndexMover implements IRedisReliableMover<Map.Entry<Long, Long>> {

    private String element;
    private SecureRandom random = new SecureRandom();

    @Override public String type() {
        return "list";
    }

    @Override public Map.Entry<Long, Long> verify(IRedisClient client, String source, String dest, Map.Entry<Long, Long> indices) {
        try {
            Long sindex = indices.getKey();
            Long dindex = indices.getValue();
            element = client.lindex(source, sindex);
            if (element == null) {
                return null; // TODO: Not the best way, but how else would we do it?
            }
            for (Long llen = client.llen(dest); llen <= dindex; ++llen) {
                client.rpush(dest, "");
            }
            return indices;
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String source, String dest, Map.Entry<Long, Long> indices) {
        try {
            String value = element + ";" + (new BigInteger(130, random)).toString(32);
            multi.lset(source, indices.getKey(), value);
            multi.lrem(source, 1, value);
            multi.lset(dest, indices.getValue(), element);
        }
        catch (Exception e) {
            return null;
        }
        return multi;
    }

}
