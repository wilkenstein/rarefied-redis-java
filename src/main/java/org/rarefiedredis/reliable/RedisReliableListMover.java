package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.adapter.RedisListAdapter;

import java.security.SecureRandom;
import java.math.BigInteger;

public final class RedisReliableListMover implements IRedisReliableMover<String> {

    private Long index;
    private boolean ignoreDestIndex;
    private SecureRandom random = new SecureRandom();

    public RedisReliableListMover() {
        this(false);
    }

    public RedisReliableListMover(boolean ignoreDestIndex) {
        this.ignoreDestIndex = ignoreDestIndex;
    }

    @Override public String type() {
        return "list";
    }

    @Override public String verify(IRedisClient client, String source, String dest, String element) {
        try {
            if (element == null) {
                index = -1L;
                return client.lindex(source, -1L);
            }
            index = (long)(new RedisListAdapter(client, source)).indexOf(element);
            if (index == -1) {
                return null;
            }
            return client.lindex(source, index);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override public IRedisClient multi(IRedisClient multi, String source, String dest, String get) {
        try {
            if (index == -1L) {
                multi.rpoplpush(source, dest);
            }
            else {
                String value = get + ";" + (new BigInteger(130, random)).toString(32);
                multi.lset(source, index, value);
                multi.lrem(source, 1, value);
                if (ignoreDestIndex) {
                    multi.lpush(dest, get);
                }
                else {
                    multi.lset(dest, index, get);
                }
            }
        }
        catch (Exception e) {
            return null;
        }
        return multi;
    }

}
