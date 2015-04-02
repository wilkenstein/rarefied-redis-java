package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;
import org.rarefiedredis.concurrency.RedisCheckAndSet;
import org.rarefiedredis.concurrency.IRedisCheckAndSet;

import java.util.List;

public final class RedisReliableDeleter {

    private RedisCheckAndSet cas;

    public RedisReliableDeleter(IRedisClient client) {
        cas = new RedisCheckAndSet(client);
    }

    private final class RedisReliableDeleterCheckAndSet<T> implements IRedisCheckAndSet<T> {

        public T deleted;
        public boolean wrongType;
        private T element;
        private IRedisReliableDeleter<T> deleter;

        public RedisReliableDeleterCheckAndSet(IRedisReliableDeleter<T> deleter, T element) {
            this.deleter = deleter;
            this.element = element;
            this.wrongType = false;
            this.deleted = null;
        }

        @Override public T get(IRedisClient client, String key) {
            String type = deleter.type();
            String t;
            if (type != null) {
                try {
                    t = client.type(key);
                }
                catch (Exception e) {
                    return null;
                }
                if (t.equals("none")) {
                    return null;
                }
                if (!t.equals(type)) {
                    wrongType = true;
                    return null;
                }
            }
            return deleter.verify(client, key, element);
        }

        @Override public IRedisClient set(IRedisClient multi, String key, T get) {
            if (get == null || !get.equals(element)) {
                return multi;
            }
            deleted = get;
            return deleter.multi(multi, key, get);
        }

    }

    public <T> T delete(IRedisReliableDeleter<T> deleter, String key, T element) throws WrongTypeException {
        RedisReliableDeleterCheckAndSet<T> cs = new RedisReliableDeleterCheckAndSet<T>(deleter, element);
        List<Object> replies = cas.checkAndSet(cs, key);
        if (cs.wrongType) {
            throw new WrongTypeException();
        }
        return (replies == null ? null : cs.deleted);
    }

}