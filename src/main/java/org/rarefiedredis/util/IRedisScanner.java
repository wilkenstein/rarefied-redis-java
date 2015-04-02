package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;

public interface IRedisScanner<T> {

    boolean hasNext(IRedisClient client, String key);

    T next(IRedisClient client, String key, String ... options);

    void range(T range);

    String type();

}
