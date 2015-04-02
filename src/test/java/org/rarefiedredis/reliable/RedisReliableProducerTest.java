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

public class RedisReliableProducerTest {

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

    @Test public void stringProducerShouldProduceAStringElement() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.StringProducer(client);
        String k = rander.randkey();
        String v = "v";
        assertEquals(1, producer.produce(k, v).size());
        assertEquals(v, client.get(k));
    }

    @Test public void stringProducerShouldNotProduceAnythingIfKeyIsNotAString() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.StringProducer(client);
        String k = rander.randkey();
        String v = "v";
        client.lpush(k, v);
        try {
            producer.produce(k, v);
        }
        catch (WrongTypeException e) {
            assertEquals("list", client.type(k));
            assertEquals(v, client.lindex(k, 0L));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(true, false);
    }

    @Test public void listProducerShouldProduceAListValue() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListProducer(client);
        String k = rander.randkey();
        String v = "v";
        assertEquals(1, producer.produce(k, v).size());
        assertEquals(1L, (long)client.llen(k));
        assertEquals(v, client.lindex(k, 0L));
    }

    @Test public void listProducerShouldProduceListValues() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListProducer(client);
        String k = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        assertEquals(3, producer.produce(k, v1, v2, v3).size());
        assertEquals(3L, (long)client.llen(k));
        List<String> range = client.lrange(k, 0L, -1L);
        assertEquals(3, range.size());
        assertEquals(v3, range.get(0));
        assertEquals(v2, range.get(1));
        assertEquals(v1, range.get(2));
    }

    @Test public void listProducerShouldNotProduceAnythingIfKeyIsNotAList() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListProducer(client);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        try {
            producer.produce(k, v);
        }
        catch (WrongTypeException e) {
            assertEquals("string", client.type(k));
            assertEquals(v, client.get(k));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(true, false);
    }

    @Test public void listRpushProducerShouldProduceAListValue() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListRpushProducer(client);
        String k = rander.randkey();
        String v = "v";
        assertEquals(1, producer.produce(k, v).size());
        assertEquals(1L, (long)client.llen(k));
        assertEquals(v, client.lindex(k, 0L));
    }

    @Test public void listRpushProducerShouldProduceListValues() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListRpushProducer(client);
        String k = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        assertEquals(3, producer.produce(k, v1, v2, v3).size());
        assertEquals(3L, (long)client.llen(k));
        List<String> range = client.lrange(k, 0L, -1L);
        assertEquals(3, range.size());
        assertEquals(v1, range.get(0));
        assertEquals(v2, range.get(1));
        assertEquals(v3, range.get(2));
    }

    @Test public void listRpushProducerShouldNotProduceAnythingIfKeyIsNotAList() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableProducer<String> producer = RedisReliableProducer.ListRpushProducer(client);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        try {
            producer.produce(k, v);
        }
        catch (WrongTypeException e) {
            assertEquals("string", client.type(k));
            assertEquals(v, client.get(k));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(true, false);
    }

    @Test public void boundedListProducerShouldProduceAListValue() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.BoundedListProducer(client, 10L);
        String k = rander.randkey();
        String v = "v";
        producer.produce(k, v).size();
        assertEquals(1L, (long)client.llen(k));
        assertEquals(v, client.lindex(k, 0L));
    }

    @Test public void boundedListProducerShouldProduceListValues() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.BoundedListProducer(client, 10L);
        String k = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        producer.produce(k, v1, v2, v3);
        assertEquals(3L, (long)client.llen(k));
        List<String> range = client.lrange(k, 0L, -1L);
        assertEquals(3, range.size());
        assertEquals(v3, range.get(0));
        assertEquals(v2, range.get(1));
        assertEquals(v1, range.get(2));
    }

    @Test public void boundedListProducerShouldNotProduceAnythingIfKeyIsNotAList() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableProducer<String> producer = RedisReliableProducer.BoundedListProducer(client, 10L);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        try {
            producer.produce(k, v);
        }
        catch (WrongTypeException e) {
            assertEquals("string", client.type(k));
            assertEquals(v, client.get(k));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(true, false);
    }

    @Test public void boundedListProducerShouldBoundTheList() throws WrongTypeException, NotImplementedException {
        RedisReliableProducer<String> producer = RedisReliableProducer.BoundedListProducer(client, 10L);
        String k = rander.randkey();
        List<String> vs = new ArrayList<String>(11);
        for (int i = 0; i < 11; ++i) {
            vs.add(String.valueOf(i));
        }
        producer.produce(k, vs.toArray(new String[0]));
        assertEquals(10L, (long)client.llen(k));
    }

}
