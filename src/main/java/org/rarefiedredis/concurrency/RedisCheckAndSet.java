package org.rarefiedredis.concurrency;

import org.rarefiedredis.redis.IRedisClient;

import java.util.List;

public final class RedisCheckAndSet {

    private IRedisClient client;

    public RedisCheckAndSet(IRedisClient client) {
        this.client = client;
    }

    public <T> List<Object> checkAndSet(IRedisCheckAndSet<T> cas, String key) {
        IRedisClient client = this.client.createClient();
        if (client == null) {
            return null;
        }
        List<Object> replies = null;
        try {
            client.watch(key);
            T value = cas.get(client, key);
            IRedisClient multi = client.multi();
            multi = cas.set(multi, key, value);
            replies = multi.exec();
        }
        catch (Exception e) {
            replies = null;
        }
        finally {
            client.close();
        }
        return replies;
    }

}