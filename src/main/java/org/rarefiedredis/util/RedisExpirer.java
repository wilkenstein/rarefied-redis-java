package org.rarefiedredis.util;

import org.rarefiedredis.redis.IRedisClient;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public final class RedisExpirer {

    private IRedisClient client;
    private Map<String, Map<String, Timer>> expiries;
    private String listKey;
    private String separator;
    private IRedisExpirer expirer;
    private RedisScanner scanner;
    private Timer timer;

    public RedisExpirer(IRedisClient client, IRedisExpirer expirer, String listKey) {
        this(client, expirer, listKey, ";");
    }

    public RedisExpirer(IRedisClient client, IRedisExpirer expirer, String listKey, String separator) {
        this.client = client;
        this.expiries = new HashMap<String, Map<String, Timer>>();
        this.listKey = listKey;
        this.separator = separator;
        this.expirer = expirer;
        this.scanner = new RedisScanner(client);
        timer = new Timer();
    }

    public RedisExpirer expire(final String key, final int timeout, final String element) {
        cleanup(key, element);
        try {
            final String id = UUID.randomUUID().toString();
            final String lelem = id + separator + key + separator + element;
            IRedisClient multi = client.multi();
            multi.setex(id, timeout, element);
            multi.rpush(listKey, lelem);
            multi.exec();
            synchronized (this) {
                if (!expiries.containsKey(key)) {
                    expiries.put(key, new HashMap<String, Timer>());
                }
            }
            synchronized (this) {
                expiries.get(key).put(element, timer);
            }
            timer.schedule(new TimerTask() {
                    @Override public void run() {
                        expired(key, element, lelem);
                    }
                }, timeout*1000);
        }
        catch (Exception e) {
            expirer.expireError(e);
        }
        return this;
    }

    public RedisExpirer pexpire(final String key, final long timeout, final String element) {
        cleanup(key, element);
        try {
            final String id = UUID.randomUUID().toString();
            final String lelem = id + separator + key + separator + element;
            IRedisClient multi = client.multi();
            multi.psetex(id, timeout, element);
            multi.rpush(listKey, lelem);
            multi.exec();
            synchronized (this) {
                if (!expiries.containsKey(key)) {
                    expiries.put(key, new HashMap<String, Timer>());
                }
            }
            synchronized (this) {
                expiries.get(key).put(element, timer);
            }
            timer.schedule(new TimerTask() {
                    @Override public void run() {
                        expired(key, element, lelem);
                    }
                }, timeout);
        }
        catch (Exception e) {
            expirer.expireError(e);
        }
        return this;
    }

    private final class PersistListScanner extends AbstractRedisListScanner {

        private String key;
        private String element;

        public PersistListScanner(String key, String element) {
            this.key = key;
            this.element = element;
        }

        @Override public void range(List<String> range) {
            for (String idkeyelement : range) {
                String[] split = idkeyelement.split(separator);
                String id = split[0];
                String k = split[1];
                String e = split[2];
                if (k.equals(key) && e.equals(element)) {
                    try {
                        IRedisClient multi = client.multi();
                        multi.del(id);
                        multi.lrem(listKey, 1L, idkeyelement);
                        multi.exec();
                        cleanup(key, element);
                        expirer.persisted(key, element);
                    }
                    catch (Exception x) {
                    }
                }
            }
        }

    }

    public synchronized RedisExpirer persist(final String key, final String element) {
        PersistListScanner lscanner = new PersistListScanner(key, element);
        try {
            scanner.scan(lscanner, listKey);
        }
        catch (Exception e) {
            expirer.persistError(e);
        }
        return this;
    }

    private final class ExistsListScanner extends AbstractRedisListScanner {

        private String key;
        private String element;
        private boolean exists;

        public ExistsListScanner(String key, String element) {
            this.key = key;
            this.element = element;
            this.exists = false;
        }

        @Override public void range(List<String> range) {
            for (String idkeyelement : range) {
                String[] split = idkeyelement.split(separator);
                String k = split[1];
                String e = split[2];
                if (k.equals(key) && e.equals(element)) {
                    exists = true;
                    expirer.exists(key, element);
                }
            }
        }

    }

    public RedisExpirer exists(final String key, final String element) {
        ExistsListScanner lscanner = new ExistsListScanner(key, element);
        try {
            scanner.scan(lscanner, listKey);
            if (!lscanner.exists) {
                expirer.doesNotExist(key, element);
            }
        }
        catch (Exception e) {
            expirer.existsError(e);
        }
        return this;
    }

    private final class CheckListScanner extends AbstractRedisListScanner {

        @Override public void range(List<String> range) {
            for (String idkeyelement : range) {
                String[] split = idkeyelement.split(separator);
                String id = split[0];
                String key = split[1];
                String element = split[2];
                try {
                    Boolean exists = client.exists(id);
                    if (!exists) {
                        expired(key, element, idkeyelement);
                    }
                }
                catch (Exception e) {
                    expirer.expired(key, element);
                }
            }
        }

    }

    public RedisExpirer check() {
        CheckListScanner lscanner = new CheckListScanner();
        try {
            scanner.scan(lscanner, listKey);
        }
        catch (Exception e) {
            expirer.checkError(e);
        }
        return this;
    }

    private synchronized void cleanup(final String key, final String element) {
        if (expiries.containsKey(key) && expiries.get(key).containsKey(element)) {
            expiries.get(key).get(element).cancel();
            expiries.get(key).remove(element);
        }
    }

    private void expired(final String key, final String element, final String lelem) {
        expirer.expired(key, element);
        try {
            client.lrem(listKey, 1L, lelem);
        }
        catch (Exception e) {
        }
        cleanup(key, element);
    }

}
