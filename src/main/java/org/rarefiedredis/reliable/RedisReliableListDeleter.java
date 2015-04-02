package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.adapter.RedisListAdapter;

import java.security.SecureRandom;
import java.math.BigInteger;

public final class RedisReliableListDeleter implements IRedisReliableDeleter<String> {

    private int index;
    private SecureRandom random = new SecureRandom();

    @Override public String type() {
        return "list";
    }

    @Override public String verify(IRedisClient client, String key, String element) { 
        try {
            index = new RedisListAdapter(client, key).indexOf(element);
            if (index == -1) {
                return null;
            }
            return element;
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String key, String element) {
        try {
            String value = element + ";" + (new BigInteger(130, random)).toString(32);
            multi.lset(key, (long)index, value);
            multi.lrem(key, 1L, value);
        }
        catch (Exception e) {
        }
        return multi;
    }
}
