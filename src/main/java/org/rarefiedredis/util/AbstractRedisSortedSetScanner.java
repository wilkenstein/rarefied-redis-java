package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.ScanResult;
import org.rarefiedredis.redis.IRedisSortedSet.ZsetPair;

import java.util.Set;
import java.util.HashSet;

public abstract class AbstractRedisSortedSetScanner implements IRedisScanner<Set<ZsetPair>> {

    private long cursor = 0L;
    private boolean done = false;

    @Override public boolean hasNext(final IRedisClient client, final String key) {
        return !done;
    }

    @Override public Set<ZsetPair> next(final IRedisClient client, final String key, final String ... options) {
        long count = 10L;
        for (int i = 0; i < options.length; ++i) {
            if (options[i].equals("count")) {
                count = Long.valueOf(options[i + 1]);
            }
        }
        Set<ZsetPair> range = new HashSet<ZsetPair>();
        try {
            ScanResult<Set<ZsetPair>> result = client.zscan(key, cursor, options);
            range = result.results;
            cursor = result.cursor;
        }
        catch (Exception e) {
            done = true;
        }
        if (cursor == 0L) {
            done = true;
        }
        return range;
    }

    // range is abstract.

    @Override public String type() {
        return "set";
    }

}
