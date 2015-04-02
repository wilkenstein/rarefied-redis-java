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

public class RedisScannerTest {

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

    @Test public void listScannerShouldErrorIfKeyIsNotAList() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        IRedisScanner<List<String>> lscanner = new AbstractRedisListScanner() {
                @Override public void range(List<String> range) {
                    assertEquals(true, false);
                }
            };
        RedisScanner scanner = new RedisScanner(client);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        try {
            scanner.scan(lscanner, k);
        }
        catch (WrongTypeException e) {
            assertEquals(v, client.get(k));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    private class ListScanner extends AbstractRedisListScanner {
        public List<String> scanned = new LinkedList<String>();
        @Override public void range(List<String> range) {
            scanned.addAll(range);
        }
    }

    @Test public void listScannerShouldScanThroughAList() throws WrongTypeException, NotImplementedException {
        List<String> vs = new ArrayList<String>(37);
        for (int i = 0; i < 37; ++i) {
            vs.add(String.valueOf(i));
        }
        RedisScanner scanner = new RedisScanner(client);
        ListScanner lscanner = new ListScanner();
        String k = rander.randkey();
        for (String v : vs) {
            client.rpush(k, v);
        }
        scanner.scan(lscanner, k);
        assertEquals(lscanner.scanned.size(), vs.size());
        for (String v : lscanner.scanned) {
            assertEquals(true, vs.contains(v));
        }
    }

    @Test public void listScannerShouldScanThroughAListWithCount() throws WrongTypeException, NotImplementedException {
        List<String> vs = new ArrayList<String>(37);
        for (int i = 0; i < 37; ++i) {
            vs.add(String.valueOf(i));
        }
        RedisScanner scanner = new RedisScanner(client);
        ListScanner lscanner = new ListScanner();
        String k = rander.randkey();
        for (String v : vs) {
            client.rpush(k, v);
        }
        scanner.scan(lscanner, k, "count", String.valueOf(8));
        assertEquals(lscanner.scanned.size(), vs.size());
        for (String v : lscanner.scanned) {
            assertEquals(true, vs.contains(v));
        }
    }

    @Test public void setScannerShouldErrorIfKeyIsNotAList() throws WrongTypeException, SyntaxErrorException, NotImplementedException {
        IRedisScanner<Set<String>> sscanner = new AbstractRedisSetScanner() {
                @Override public void range(Set<String> range) {
                    assertEquals(true, false);
                }
            };
        RedisScanner scanner = new RedisScanner(client);
        String k = rander.randkey();
        String v = "v";
        client.set(k, v);
        try {
            scanner.scan(sscanner, k);
        }
        catch (WrongTypeException e) {
            assertEquals(v, client.get(k));
            return;
        }
        catch (Exception e) {
        }
        assertEquals(false, true);
    }

    private class SetScanner extends AbstractRedisSetScanner {
        public Set<String> scanned = new HashSet<String>();
        @Override public void range(Set<String> range) {
            scanned.addAll(range);
        }
    }

    @Test public void setScannerShouldScanThroughASet() throws WrongTypeException, NotImplementedException {
        Set<String> vs = new HashSet<String>();
        for (int i = 0; i < 37; ++i) {
            vs.add(String.valueOf(i));
        }
        RedisScanner scanner = new RedisScanner(client);
        SetScanner sscanner = new SetScanner();
        String k = rander.randkey();
        for (String v : vs) {
            client.sadd(k, v);
        }
        scanner.scan(sscanner, k);
        assertEquals(sscanner.scanned.size(), vs.size());
        for (String v : sscanner.scanned) {
            assertEquals(true, vs.contains(v));
        }
    }

    @Test public void setScannerShouldScanThroughASetWithCount() throws WrongTypeException, NotImplementedException {
        Set<String> vs = new HashSet<String>();
        for (int i = 0; i < 37; ++i) {
            vs.add(String.valueOf(i));
        }
        RedisScanner scanner = new RedisScanner(client);
        SetScanner sscanner = new SetScanner();
        String k = rander.randkey();
        for (String v : vs) {
            client.sadd(k, v);
        }
        scanner.scan(sscanner, k, "count", String.valueOf(8));
        assertEquals(sscanner.scanned.size(), vs.size());
        for (String v : sscanner.scanned) {
            assertEquals(true, vs.contains(v));
        }
    }

}