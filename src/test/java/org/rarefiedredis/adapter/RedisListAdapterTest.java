package org.rarefiedredis.adapter;

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

import java.util.List;
import java.util.ArrayList;

public class RedisListAdapterTest {

    private IRedisClient client;
    private RandomKey rander;

    @Before public void initClient() {
        // TODO: Switch to jedis adapter on integration tests.
        client = new RedisMock();
        rander = new RandomKey();
    }

    @Test public void indexOfShouldReturnNegOneForAKeyThatDoesNotExist() {
        String k = rander.randkey();
        String v = "v";
        RedisListAdapter adapter = new RedisListAdapter(client, k);
        assertEquals(-1, adapter.indexOf(v));
    }

    @Test public void indexOfShouldThrowNullPointerExceptionIfElementIsNull() {
        String k = rander.randkey();
        String v = "v";
        RedisListAdapter adapter = new RedisListAdapter(client, k);
        try {
            int idx = adapter.indexOf(null);
        }
        catch (NullPointerException e) {
            assertEquals(true, true);
            return;
        }
        assertEquals(false, true);
    }

    @Test public void indexOfShouldThrowClassCastExceptionIfElementIsNotAString() {
        String k = rander.randkey();
        String v = "v";
        RedisListAdapter adapter = new RedisListAdapter(client, k);
        try {
            int idx = adapter.indexOf(1);
        }
        catch (ClassCastException e) {
            assertEquals(true, true);
            return;
        }
        assertEquals(false, true);
    }

    @Test public void indexOfShouldReturnTheIndexOfTheElementInTheList() {
        String k = rander.randkey();
        String v = "v", v2 = "v2", v3 = "v3", v4 = "v2", v5 = "v5";
        RedisListAdapter adapter = new RedisListAdapter(client, k);
        try {
            client.lpush(k, v);
            assertEquals(0, adapter.indexOf(v));
            assertEquals(v, client.lindex(k, (long)adapter.indexOf(v)));
            client.rpush(k, v2, v3, v4, v5);
            assertEquals(1, adapter.indexOf(v2));
            assertEquals(2, adapter.indexOf(v3));
            assertEquals(1, adapter.indexOf(v2)); // v4 == v2
            assertEquals(4, adapter.indexOf(v5));
            assertEquals(-1, adapter.indexOf("v6"));
        }
        catch (Exception e) {
            assertEquals(false, true);
        }
    }

}