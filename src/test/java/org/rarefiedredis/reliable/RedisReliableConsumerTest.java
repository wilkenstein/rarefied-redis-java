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

public class RedisReliableConsumerTest {

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

    @Test public void consumeShouldDoNothingForANonExistentKey() throws WrongTypeException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.StringConsumer(client);
        assertEquals(null, consumer.consume(k, ik));
    }

    @Test public void consumeShouldNotConsumeIfTheShadowStructureIsNotTheSameTypeAsTheOriginalStructure() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.StringConsumer(client);
        client.lpush(ik, v);
        client.set(k, v);
        try {
            consumer.consume(k, ik);
        }
        catch (WrongTypeException e) {
            assertEquals(v, client.get(k));
            assertEquals(v, client.lindex(ik, 0L));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    @Test public void consumeShouldReliablyConsumeAStringElement() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.StringConsumer(client);
        client.set(k, v);
        assertEquals(v, consumer.consume(k, ik));
        assertEquals(null, client.get(k));
        assertEquals(v, client.get(ik));
    }

    @Test public void consumeShouldReliablyConsumeAPopListElement() throws WrongTypeException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.ListConsumer(client);
        client.rpush(k, v1, v2, v3);
        assertEquals(v3, consumer.consume(k, ik));
        assertEquals(v3, client.lindex(ik, 0L));
        assertEquals(2L, (long)client.llen(k));
        assertEquals(v2, consumer.consume(k, ik));
        assertEquals(v2, client.lindex(ik, 0L));
        assertEquals(1L, (long)client.llen(k));
        assertEquals(2L, (long)client.llen(ik));
    }

    @Test public void consumeShouldReliablyConsumeASetElement() throws WrongTypeException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.SetConsumer(client);        
        client.sadd(k, v1, v2, v3);
        assertEquals(v2, consumer.consume(k, ik, v2));
        assertEquals(true, client.exists(ik));
        assertEquals(1L, (long)client.scard(ik));
        assertEquals(2L, (long)client.scard(k));
        assertEquals(true, client.sismember(ik, v2));
        assertEquals(false, client.sismember(k, v2));
        assertEquals(v1, consumer.consume(k, ik, v1));
        assertEquals(2L, (long)client.scard(ik));
        assertEquals(1L, (long)client.scard(k));
        assertEquals(true, client.sismember(ik, v1));
        assertEquals(false, client.sismember(k, v1));
    }

    @Test public void consumeShouldReliablyConsumeARandomSetElement() throws WrongTypeException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        List<String> vs = new ArrayList<String>(3);
        vs.add(v1);
        vs.add(v2);
        vs.add(v3);
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.SetConsumer(client);
        client.sadd(k, v1, v2, v3);
        String consumed = consumer.consume(k, ik);
        assertEquals(true, vs.contains(consumed));
        assertEquals(true, client.exists(ik));
        assertEquals(1L, (long)client.scard(ik));
        assertEquals(2L, (long)client.scard(k));
        assertEquals(true, client.sismember(ik, consumed));
        assertEquals(false, client.sismember(k, consumed));
        consumed = consumer.consume(k, ik);
        assertEquals(true, vs.contains(consumed));
        assertEquals(2L, (long)client.scard(ik));
        assertEquals(1L, (long)client.scard(k));
        assertEquals(true, client.sismember(ik, consumed));
        assertEquals(false, client.sismember(k, consumed));
    }

    @Test public void ackShouldReliablyConsumeAndAckAStringElement() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.StringConsumer(client);
        client.set(k, v);
        assertEquals(v, consumer.consume(k, ik));
        assertEquals(v, client.get(ik));
        assertEquals(null, client.get(k));
        assertEquals(v, consumer.ack(k, ik, v));
        assertEquals(null, client.get(ik));
        assertEquals(null, client.get(k));
    }

    @Test public void ackShouldReliablyConsumeAndAckAPopListElement() throws WrongTypeException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.ListConsumer(client);
        client.rpush(k, v1, v2, v3);
        assertEquals(v3, consumer.consume(k, ik));
        assertEquals(v3, client.lindex(ik, 0L));
        assertEquals(2L, (long)client.llen(k));
        assertEquals(1L, (long)client.llen(ik));
        assertEquals(v2, consumer.consume(k, ik));
        assertEquals(1L, (long)client.llen(k));
        assertEquals(2L, (long)client.llen(ik));
        assertEquals(v2, consumer.ack(k, ik, v2));
        assertEquals(1L, (long)client.llen(ik));
        assertEquals(1L, (long)client.llen(k));
        assertEquals(0L, (long)client.lrem(ik, 0, v2));
        assertEquals(v3, consumer.ack(k, ik, v3));
        assertEquals(0L, (long)client.llen(ik));
        assertEquals(1L, (long)client.llen(k));
        assertEquals(0L, (long)client.lrem(ik, 0, v3));
    }

    @Test public void failShouldReliablyConsumeAndFailAStringElement() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.StringConsumer(client);
        client.set(k, v);
        assertEquals(v, consumer.consume(k, ik));
        assertEquals(null, client.get(k));
        assertEquals(v, client.get(ik));
        assertEquals(v, consumer.fail(k, ik, v));
        assertEquals(null, client.get(ik));
        assertEquals(v, client.get(k));
    }

    @Test public void failShouldReliablyConsumeAndFailAPopListElement() throws WrongTypeException, NotImplementedException {
        String k = rander.randkey(), ik = rander.randkey();
        String v = "v";
        String v1 = "v1", v2 = "v2", v3 = "v3";
        RedisReliableConsumer<String> consumer = RedisReliableConsumer.ListConsumer(client);
        List<String> klst = new RedisListAdapter(client, k);
        List<String> iklst = new RedisListAdapter(client, ik);
        client.rpush(k, v1, v2, v3);
        assertEquals(v3, consumer.consume(k, ik));
        assertEquals(v3, client.lindex(ik, 0L));
        assertEquals(2L, (long)client.llen(k));
        assertEquals(1L, (long)client.llen(ik));
        assertEquals(v2, consumer.consume(k, ik));
        assertEquals(1L, (long)client.llen(k));
        assertEquals(2L, (long)client.llen(ik));
        assertEquals(v2, consumer.fail(k, ik, v2));
        assertEquals(2L, (long)client.llen(k));
        assertEquals(1L, (long)client.llen(ik));
        assertEquals(true, klst.contains(v2));
        assertEquals(false, iklst.contains(v2));
        assertEquals(v3, consumer.fail(k, ik, v3));
        assertEquals(3L, (long)client.llen(k));
        assertEquals(0L, (long)client.llen(ik));
        assertEquals(true, klst.contains(v3));
        assertEquals(false, iklst.contains(v3));
    }

}
