package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;
import org.rarefiedredis.concurrency.RedisCheckAndSet;
import org.rarefiedredis.concurrency.IRedisCheckAndSet;

import java.util.List;

public final class RedisReliableMover {

    private RedisCheckAndSet cas;

    public RedisReliableMover(IRedisClient client) {
        cas = new RedisCheckAndSet(client);
    }

    private final class RedisReliableMoverCheckAndSet<T> implements IRedisCheckAndSet<T> {

        public T moved;
        public boolean wrongType;
        private String source;
        private String dest;
        private T element;
        private IRedisReliableMover<T> mover;

        public RedisReliableMoverCheckAndSet(IRedisReliableMover<T> mover, String source, String dest, T element) {
            this.mover = mover;
            this.source = source;
            this.dest = dest;
            this.element = element;
            this.moved = null;
            this.wrongType = false;
        }

        @Override public T get(IRedisClient client, String key) {
            String type = mover.type();
            String t;
            if (type != null) {
                try {
                    t = client.type(dest);
                }
                catch (Exception e) {
                    return null;
                }
                if (!t.equals("none") && !t.equals(type)) {
                    wrongType = true;
                    return null;
                }
            }
            return mover.verify(client, source, dest, element);
        }

        @Override public IRedisClient set(IRedisClient multi, String key, T get) {
            if (get == null) {
                return multi;
            }
            moved = get;
            return mover.multi(multi, source, dest, get);
        }

    }

    public <T> T move(final IRedisReliableMover<T> mover, final String source, final String dest) throws WrongTypeException {
        return move(mover, source, dest, null);
    }

    public <T> T move(final IRedisReliableMover<T> mover, final String source, final String dest, final T element) throws WrongTypeException {
        RedisReliableMoverCheckAndSet<T> cs = new RedisReliableMoverCheckAndSet<T>(mover, source, dest, element);
        List<Object> replies = cas.checkAndSet(cs, source);
        if (cs.wrongType) {
            throw new WrongTypeException();
        }
        return (replies == null ? null : cs.moved);
    }

}