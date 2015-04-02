package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;
import org.rarefiedredis.concurrency.RedisCheckAndSet;
import org.rarefiedredis.concurrency.IRedisCheckAndSet;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

public final class RedisReliableProducer<T> {

    private RedisCheckAndSet cas;
    private IRedisReliableProducer<T> iproducer;

    public RedisReliableProducer(IRedisClient client, IRedisReliableProducer<T> iproducer) {
        cas = new RedisCheckAndSet(client);
        this.iproducer = iproducer;
    }

    private final class RedisReliableProducerCheckAndSet<T> implements IRedisCheckAndSet<T[]> {

        private T[] productions;
        private boolean wrongType;
        private IRedisReliableProducer<T> producer;

        public RedisReliableProducerCheckAndSet(IRedisReliableProducer<T> producer, T[] productions) {
            this.productions = productions;
            this.producer = producer;
            wrongType = false;
        }

        @Override public T[] get(IRedisClient client, String key) {
            String type = producer.type();
            String t;
            if (type != null) {
                try {
                    t = client.type(key);
                }
                catch (Exception e) {
                    return null;
                }
                if (!t.equals("none") && !t.equals(type)) {
                    wrongType = true;
                    return null;
                }
            }
            return productions;
        }

        @Override public IRedisClient set(IRedisClient multi, String key, T[] get) {
            if (wrongType) {
                return multi;
            }
            for (T production : get) {
                multi = producer.multi(multi, key, production);
            }
            return multi;
        }

    }

    public List<Object> produce(final String key, final T ... productions) throws WrongTypeException {
        RedisReliableProducerCheckAndSet<T> cs = new RedisReliableProducerCheckAndSet<T>(iproducer, productions);
        List<Object> replies = cas.checkAndSet(cs, key);
        if (cs.wrongType) {
            throw new WrongTypeException();
        }
        return replies;
    }

    public static RedisReliableProducer<String> StringProducer(IRedisClient client) {
        return new RedisReliableProducer<String>(client, new RedisReliableStringProducer());
    }

    public static RedisReliableProducer<String> ListProducer(IRedisClient client) {
        return new RedisReliableProducer<String>(client, new RedisReliableListProducer());
    }

    public static RedisReliableProducer<String> ListRpushProducer(IRedisClient client) {
        return new RedisReliableProducer<String>(client, new RedisReliableListRpushProducer());
    }

    public static RedisReliableProducer<String> BoundedListProducer(IRedisClient client, long bound) {
        return new RedisReliableProducer<String>(client, new RedisReliableBoundedListProducer(bound));
    }

}