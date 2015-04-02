package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;

public final class RedisScanner {

    private IRedisClient client;

    public RedisScanner(IRedisClient client) {
        this.client = client;
    }

    public <T> RedisScanner scan(final IRedisScanner<T> scanner, final String key, final String ... options) throws WrongTypeException {
        String type = scanner.type();
        String t = "none";
        if (key != null && type != null) {
            try {
                t = client.type(key);
            }
            catch (Exception e) {
                return this;
            }
            if (!t.equals("none") && !t.equals(type)) {
                throw new WrongTypeException();
            }
        }
        while (scanner.hasNext(client, key)) {
            scanner.range(scanner.next(client, key, options));
            Thread.yield();
        }
        return this;
    }

}
