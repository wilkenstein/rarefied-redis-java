package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;

import java.util.List;
import java.util.LinkedList;

public abstract class AbstractRedisListScanner implements IRedisScanner<List<String>> {

    private long index = 0L;
    private boolean done = false;

    @Override public boolean hasNext(final IRedisClient client, final String key) {
        return !done;
    }

    @Override public List<String> next(final IRedisClient client, final String key, final String ... options) {
        long count = 10L;
        for (int i = 0; i < options.length; ++i) {
            if (options[i].equals("count")) {
                count = Long.valueOf(options[i + 1]);
            }
        }
        List<String> range = new LinkedList<String>();
        try {
            range = client.lrange(key, index, index + count);
        }
        catch (Exception e) {
            done = true;
        }
        if (range.isEmpty()) {
            done = true;
        }
        index = index + range.size();
        return range;
    }

    // range is abstract.

    @Override public String type() {
        return "list";
    }

}
