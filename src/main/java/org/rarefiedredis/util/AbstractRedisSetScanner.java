package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.ScanResult;

import java.util.Set;
import java.util.HashSet;

public abstract class AbstractRedisSetScanner implements IRedisScanner<Set<String>> {

    private long cursor = 0L;
    private boolean done = false;

    @Override public boolean hasNext(final IRedisClient client, final String key) {
        return !done;
    }

    @Override public Set<String> next(final IRedisClient client, final String key, final String ... options) {
        long count = 10L;
        for (int i = 0; i < options.length; ++i) {
            if (options[i].equals("count")) {
                count = Long.valueOf(options[i + 1]);
            }
        }
        Set<String> range = new HashSet<String>();
        try {
            ScanResult<Set<String>> result = client.sscan(key, cursor, options);
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
