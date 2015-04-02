package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.adapter.RedisListAdapter;

import java.security.SecureRandom;
import java.math.BigInteger;

public final class RedisReliableListIndexDeleter implements IRedisReliableDeleter<Long> {

    private String element;
    private SecureRandom random = new SecureRandom();

    @Override public String type() {
        return "list";
    }

    @Override public Long verify(IRedisClient client, String key, Long index) { 
        try {
            if (index == null) {
                return null;
            }
            element = client.lindex(key, index);
            return (element != null ? index : null);
        }
        catch (Exception e) {
            element = null;
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, Long index) {
        try {
            String value = element + ";" + (new BigInteger(130, random)).toString(32);
            multi.lset(key, index, value);
            multi.lrem(key, 1L, value);
        }
        catch (Exception e) {
        }
        return multi;
    }
}
