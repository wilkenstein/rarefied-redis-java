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
import org.rarefiedredis.redis.adapter.jedis.JedisIRedisClient;
import org.rarefiedredis.redis.IRedisSortedSet.ZsetPair;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.ArrayList;

public class RedisReliableMoverTest {

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

    @Test public void redisReliableStringMoverShouldMoveAStringFromOneKeyToANonExistentKey() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.set(s, v);
        String reply = mover.move(new RedisReliableStringMover(), s, d);
        assertEquals(v, reply);
        assertEquals(v, client.get(d));
        assertEquals(null, client.get(s));
    }

    @Test public void redisReliableStringMoverShouldMoveAStringFromOneKeyToAnother() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v", oldv = "oldv";
        client.set(s, v);
        client.set(d, oldv);
        assertEquals(v, mover.move(new RedisReliableStringMover(), s, d));
        assertEquals(v, client.get(d));
        assertEquals(null, client.get(s));
    }

    @Test public void redisReliableStringMoverShouldNotMoveAStringIntoANonStringKey() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.set(s, v);
        client.lpush(d, v);
        try {
            mover.move(new RedisReliableStringMover(), s, d);
        }
        catch (WrongTypeException wte) {
            assertEquals(v, client.get(s));
            assertEquals("list", client.type(d));
            assertEquals(v, client.lindex(d, 0L));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(true, false);
    }

    @Test public void redisReliableSetMoverShouldMoveAMemberFromOneSetToANonExistentKey() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.sadd(s, v);
        assertEquals(v, mover.move(new RedisReliableSetMover(), s, d, v));
        assertEquals("set", client.type(d));
        assertEquals(true, client.sismember(d, v));
        assertEquals(false, client.sismember(s, v));
    }

    @Test public void redisReliableSetMoverShouldMoveAMemberFromOneSetToAnother() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v", v2 = "v2";
        client.sadd(s, v);
        client.sadd(d, v2);
        assertEquals(v, mover.move(new RedisReliableSetMover(), s, d, v));
        assertEquals(true, client.sismember(d, v));
        assertEquals(false, client.sismember(s, v));
        assertEquals(2L, (long)client.scard(d));
    }

    @Test public void redisReliableSetMoverShouldNotMoveAMemberIntoANonSetKey() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.sadd(s, v);
        client.lpush(d, v);
        try {
            mover.move(new RedisReliableSetMover(), s, d, v);
        }
        catch (WrongTypeException wte) {
            assertEquals(true, client.sismember(s, v));
            assertEquals("list", client.type(d));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    @Test public void redisReliableSetMoverShouldMoveARandomMemberFromOneSetToANonExistentKey() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        List<String> vs = new ArrayList<String>(3);
        vs.add(v1);
        vs.add(v2);
        vs.add(v3);
        client.sadd(s, v1, v2, v3);
        String moved = mover.move(new RedisReliableSetMover(), s, d);
        assertEquals(true, vs.contains(moved));
        assertEquals(true, client.sismember(d, moved));
        assertEquals(false, client.sismember(s, moved));
        assertEquals(2L, (long)client.scard(s));
        assertEquals(1L, (long)client.scard(d));
    }

    @Test public void redisReliableSetMoverShouldMoveARandomMemberFromOneSetToAnother() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        List<String> vs = new ArrayList<String>(3);
        vs.add(v1);
        vs.add(v2);
        vs.add(v3);
        client.sadd(s, v1, v2, v3);
        client.sadd(d, "e");
        String moved = mover.move(new RedisReliableSetMover(), s, d);
        assertEquals(true, vs.contains(moved));
        assertEquals(true, client.sismember(d, moved));
        assertEquals(false, client.sismember(s, moved));
        assertEquals(2L, (long)client.scard(s));
        assertEquals(2L, (long)client.scard(d));
    }

    @Test public void redisReliableSetMoverShouldNotMoveARandomMemberIntoANonSetKey() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.sadd(s, v);
        client.set(d, v);
        try {
            mover.move(new RedisReliableSetMover(), s, d);
        }
        catch (WrongTypeException wte) {
            assertEquals(true, client.sismember(s, v));
            assertEquals(v, client.get(d));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    @Test public void redisReliableListMoverShouldPopAnElementFromOneListToAnother() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v11 = "v11", v12 = "v12", v13 = "v13", v21 = "v21", v22 = "v22", v23 = "v23";
        client.rpush(s, v11, v12, v13);
        client.lpush(d, v21, v22, v23);
        assertEquals(v13, mover.move(new RedisReliableListMover(), s, d));
        assertEquals(v13, client.lindex(d, 0L));
        assertEquals(v12, client.lindex(s, 1L));
        assertEquals(2L, (long)client.llen(s));
        assertEquals(4L, (long)client.llen(d));
    }

    @Test public void redisReliableListMoverShouldNotPopAnElementIntoANonListKey() throws WrongTypeException, NotImplementedException {
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v11 = "v11", v12 = "v12", v13 = "v13", v21 = "v21", v22 = "v22", v23 = "v23";
        client.rpush(s, v11, v12, v13);
        client.sadd(d, v21, v22, v23);
        try {
            mover.move(new RedisReliableListMover(), s, d);
        }
        catch (WrongTypeException e) {
            assertEquals("set", client.type(d));
            assertEquals(3L, (long)client.llen(s));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    @Test public void redisReliableSortedSetMoverShouldMoveAMemberFromOneSetToANonExistentKey() throws WrongTypeException, NotImplementedException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.zadd(s, new ZsetPair(v, 0d));
        assertEquals(v, mover.move(new RedisReliableSortedSetMover(), s, d, v));
        assertEquals("zset", client.type(d));
        assertEquals(0d, (double)client.zscore(d, v), 0.01d);
        assertEquals(null, client.zscore(s, v));
    }

    @Test public void redisReliableSortedSetMoverShouldMoveAMemberFromOneSetToAnother() throws WrongTypeException, NotImplementedException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v", v2 = "v2";
        client.zadd(s, new ZsetPair(v, 0d));
        client.zadd(d, new ZsetPair(v2, 2d));
        assertEquals(v, mover.move(new RedisReliableSortedSetMover(), s, d, v));
        assertEquals(0d, (double)client.zscore(d, v), 0.01d);
        assertEquals(null, client.zscore(s, v));
        assertEquals(2L, (long)client.zcard(d));
    }

    @Test public void redisReliableSortedSetMoverShouldNotMoveAMemberIntoANonSetKey() throws WrongTypeException, NotImplementedException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.zadd(s, new ZsetPair(v, 0d));
        client.lpush(d, v);
        try {
            mover.move(new RedisReliableSortedSetMover(), s, d, v);
        }
        catch (WrongTypeException wte) {
            assertEquals(0d, (double)client.zscore(s, v), 0.01d);
            assertEquals("list", client.type(d));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    @Test public void redisReliableSortedSetMoverShouldMoveARandomMemberFromOneSetToANonExistentKey() throws WrongTypeException, NotImplementedException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        List<ZsetPair> vs = new ArrayList<ZsetPair>(3);
        vs.add(new ZsetPair(v1, 1d));
        vs.add(new ZsetPair(v2, 2d));
        vs.add(new ZsetPair(v3, 3d));
        client.zadd(s, vs.get(0), vs.get(1), vs.get(2));
        String moved = mover.move(new RedisReliableSortedSetMover(), s, d);
        assertEquals(v1, moved);
        assertEquals(1d, (double)client.zscore(d, moved), 0.01d);
        assertEquals(null, client.zscore(s, moved));
        assertEquals(2L, (long)client.zcard(s));
        assertEquals(1L, (long)client.zcard(d));
    }

    @Test public void redisReliableSortedSetMoverShouldMoveARandomMemberFromOneSetToAnother() throws WrongTypeException, NotImplementedException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v1 = "v1", v2 = "v2", v3 = "v3";
        List<ZsetPair> vs = new ArrayList<ZsetPair>(3);
        vs.add(new ZsetPair(v1, 1d));
        vs.add(new ZsetPair(v2, 2d));
        vs.add(new ZsetPair(v3, 3d));
        client.zadd(s, vs.get(0), vs.get(1), vs.get(2));
        client.zadd(d, new ZsetPair("e", 5d));
        String moved = mover.move(new RedisReliableSortedSetMover(), s, d);
        assertEquals(v1, moved);
        assertEquals(1d, (double)client.zscore(d, moved), 0.01d);
        assertEquals(null, client.zscore(s, moved));
        assertEquals(2L, (long)client.zcard(s));
        assertEquals(2L, (long)client.zcard(d));
    }

    @Test public void redisReliableSortedSetMoverShouldNotMoveARandomMemberIntoANonSetKey() throws WrongTypeException, NotImplementedException, SyntaxErrorException {
        if (client instanceof RedisMock) {
            // TODO: zsets are not implemented in RedisMock yet.
            return;
        }
        RedisReliableMover mover = new RedisReliableMover(client);
        String s = rander.randkey(), d = rander.randkey();
        String v = "v";
        client.zadd(s, new ZsetPair(v, 1d));
        client.set(d, v);
        try {
            mover.move(new RedisReliableSortedSetMover(), s, d);
        }
        catch (WrongTypeException wte) {
            assertEquals(1d, (double)client.zscore(s, v), 0.01d);
            assertEquals(v, client.get(d));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

}