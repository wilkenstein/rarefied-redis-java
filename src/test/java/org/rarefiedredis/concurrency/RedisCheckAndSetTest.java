package org.rarefiedredis.concurrency;

import org.junit.Test;
import org.junit.Before;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;

import org.rarefiedredis.RandomKey;
import org.rarefiedredis.redis.RedisMock;
import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;
import org.rarefiedredis.redis.NotImplementedException;
import org.rarefiedredis.redis.SyntaxErrorException;
import org.rarefiedredis.redis.adapter.jedis.JedisIRedisClient;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

public class RedisCheckAndSetTest {

    private IRedisClient client, other;
    private RandomKey rander;

    @Before public void initClient() {
        rander = new RandomKey();
        String integration = System.getProperty("integration");
        if (integration != null && integration.equals("true")) {
            client = new JedisIRedisClient(new JedisPool(new JedisPoolConfig(), "localhost"));
            other = new JedisIRedisClient(new JedisPool(new JedisPoolConfig(), "localhost"));
        }
        else {
            RedisMock mock = new RedisMock();
            client = mock.createClient();
            other = mock.createClient();
        }
    }

    @Test public void checkAndSetShouldCheckAndSetAnUnchangingKey() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisCheckAndSet cas = new RedisCheckAndSet(client);
        String k = rander.randkey();
        final String v = "v", v2 = "v2";
        client.set(k, v);
        List<Object> replies = cas.checkAndSet(new IRedisCheckAndSet<String>() {
                @Override public String get(IRedisClient client, String key) {
                    try {
                        return client.get(key);
                    }
                    catch (Exception e) {
                        return null;
                    }
                }
                @Override public IRedisClient set(IRedisClient multi, String key, String get) {
                    try {
                        multi.set(key, v2);
                    }
                    catch (Exception e) {
                    }
                    return multi;
                }
            }, k);
        assertEquals(true, replies != null);
        assertEquals(1, replies.size());
        assertEquals("OK", (String)replies.get(0));
        assertEquals(v2, client.get(k));
    }

    @Test public void checkAndSetShouldFailIfAKeyChangesBeforeTheMultiIsExecd() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisCheckAndSet cas = new RedisCheckAndSet(client);
        final String k = rander.randkey();
        final String v = "v", v2 = "v2", v3 = "v3";
        client.set(k, v);
        List<Object> replies = cas.checkAndSet(new IRedisCheckAndSet<String>() {
                @Override public String get(IRedisClient client, String key) {
                    try {
                        return client.get(key);
                    }
                    catch (Exception e) {
                        return null;
                    }
                }
                @Override public IRedisClient set(IRedisClient multi, String key, String get) {
                    try {
                        other.set(k, v3);
                        multi.set(key, v2);
                    }
                    catch (Exception e) {
                    }
                    return multi;
                }
            }, k);
        assertEquals(null, replies);
    }

}