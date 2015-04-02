package org.rarefiedredis.reliable;

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
import org.rarefiedredis.adapter.RedisListAdapter;
import org.rarefiedredis.redis.adapter.jedis.JedisIRedisClient;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.ArrayList;

public class RedisReliableDeleterTest {

    private IRedisClient client;
    private RandomKey rander;

    @Before public void initClient() {
        rander = new RandomKey();
        String integration = System.getProperty("integration");
        if (integration != null && integration.equals("true")) {
            client = new JedisIRedisClient(new JedisPool(new JedisPoolConfig(), "localhost"));
        }
        else {
            client = new RedisMock();
        }
    }

    @Test public void redisReliableStringDeleterShouldDoNothingForANonExistentKey() throws WrongTypeException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v";
        assertEquals(null, deleter.delete(new RedisReliableStringDeleter(), k, v));
        assertEquals(null, client.get(k));
    }

    @Test public void redisReliableStringDeleterShouldNotDeleteTheKeyIfTheValuesDoNotMatch() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v", v2 = "v2";
        client.set(k, v);
        assertEquals(null, deleter.delete(new RedisReliableStringDeleter(), k, v2));
        assertEquals(v, client.get(k));
    }

    @Test public void redisReliableStringDeleterShouldNotDeleteTheKeyIfNoValueIsGiven() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v", v2 = "v2";
        client.set(k, v);
        assertEquals(null, deleter.delete(new RedisReliableStringDeleter(), k, null));
        assertEquals(v, client.get(k));
    }

    @Test public void redisReliableStringDeleterShouldDeleteTheKeyIfTheValueMatches() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        assertEquals(v, deleter.delete(new RedisReliableStringDeleter(), k, v));
        assertEquals(null, client.get(k));
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShouldDoNothingForANonExistentKey() {
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShouldNotDeleteTheIndexIfTheIndexIsOutOfRange() {
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShouldNotDeleteTheIndexIfNoIndexIsGiven() {
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShouldDeleteTheIndexForAnInRangeIndex() {
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShouldDeleteTheFirstOccurrenceOfAnElementInTheList() {
    }

    @Ignore("pending") @Test public void redisReliableListIndexDeleterShoulddoNothingIfTheElementIsNotInTheList() {
    }

    @Test public void redisReliableListDeleterShouldDoNothingForANonExistentKey() throws WrongTypeException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v";
        assertEquals(null, deleter.delete(new RedisReliableListDeleter(), k, v));
        assertEquals(null, client.lindex(k, 0L));
    }

    @Test public void redisReliableListDeleterShouldDoNothingIfTheElementIsNotInTheList() throws WrongTypeException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v", v2 = "v2";
        client.lpush(k, v);
        assertEquals(null, deleter.delete(new RedisReliableListDeleter(), k, v2));
        assertEquals(1L, (long)client.llen(k));
    }

    @Test public void redisReliableListDeleterShouldNotDeleteTheIfNoneIsGiven() throws WrongTypeException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v";
        client.lpush(k, v);        
        assertEquals(null, deleter.delete(new RedisReliableListDeleter(), k, null));
    }

    @Test public void redisReliableListDeleterShouldDeleteTheElementIfItIsInTheList() throws WrongTypeException, NotImplementedException {
        RedisReliableDeleter deleter = new RedisReliableDeleter(client);
        String k = rander.randkey();
        String v = "v", v1 = "v1", v2 = "v2", v3 = "v3", v4 = "v2", v5 = "v5";
        List<String> lst = new RedisListAdapter(client, k);
        client.lpush(k, v);
        assertEquals(v, deleter.delete(new RedisReliableListDeleter(), k, v));
        assertEquals(0L, (long)client.llen(k));
        client.rpush(k, v1, v2, v3, v4, v5);
        assertEquals(v3, deleter.delete(new RedisReliableListDeleter(), k, v3));
        assertEquals(-1, lst.indexOf(v3));
        assertEquals(4L, (long)client.llen(k));
        assertEquals(v2, deleter.delete(new RedisReliableListDeleter(), k, v2));
        assertEquals(3L, (long)client.llen(k));
        assertEquals(1, lst.indexOf("v2"));
        assertEquals(v4, deleter.delete(new RedisReliableListDeleter(), k, v4));
        assertEquals(2L, (long)client.llen(k));
        assertEquals(-1, lst.indexOf("v2"));
        assertEquals(0, lst.indexOf(v1));
        assertEquals(1, lst.indexOf(v5));
    }

}
