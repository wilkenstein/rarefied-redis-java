package org.rarefiedredis.util;

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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;

public class RedisExpirerTest {

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

    private final class TestRedisExpirer implements IRedisExpirer {

        public String functionCalled;
        public String key;
        public String value;
        public Exception exception;

        @Override public void expired(String key, String value) {
            functionCalled = "expired";
            this.key = key;
            this.value = value;
        }

        @Override public void persisted(String key, String value) {
            functionCalled = "persisted";
            this.key = key;
            this.value = value;
        }

        @Override public void exists(String key, String value) {
            functionCalled = "exist";
            this.key = key;
            this.value = value;
        }

        @Override public void doesNotExist(String key, String value) {
            functionCalled = "doesNotExist";
            this.key = key;
            this.value = value;
        }

        @Override public void expireError(Exception e) {
            functionCalled = "expireError";
            this.exception = e;
        }

        @Override public void persistError(Exception e) {
            functionCalled = "persistError";
            this.exception = e;
        }

        @Override public void checkError(Exception e) {
            functionCalled = "checkError";
            this.exception = e;
        }

        @Override public void existsError(Exception e) {
            functionCalled = "existsError";
            this.exception = e;
        }

    }

    @Test public void expireShouldExpireAListElement() throws WrongTypeException, NotImplementedException, InterruptedException {
        String k = rander.randkey(), lk = rander.randkey();
        String v = "v";
        TestRedisExpirer uat = new TestRedisExpirer();
        RedisExpirer expirer = new RedisExpirer(client, uat, lk);
        client.lpush(k, v);
        expirer.expire(k, 1, v);
        assertEquals(1L, (long)client.llen(lk));
        Thread.sleep(1500);
        assertEquals("expired", uat.functionCalled);
        assertEquals(k, uat.key);
        assertEquals(v, uat.value);
    }

    @Test public void persistShouldSetAnExpiryPersistTheElementAndNotExpireIt() throws WrongTypeException, NotImplementedException, InterruptedException {
        String k = rander.randkey(), lk = rander.randkey();
        String v = "v";
        TestRedisExpirer uat = new TestRedisExpirer();
        RedisExpirer expirer = new RedisExpirer(client, uat, lk);
        client.lpush(k, v);
        expirer.expire(k, 1, v);
        assertEquals(1L, (long)client.llen(lk));
        Thread.sleep(250);
        expirer.persist(k, v);
        Thread.sleep(900);
        assertEquals("persisted", uat.functionCalled);
        assertEquals(k, uat.key);
        assertEquals(v, uat.value);
    }

}