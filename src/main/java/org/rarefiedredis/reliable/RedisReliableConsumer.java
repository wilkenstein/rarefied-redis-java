package org.rarefiedredis.reliable;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.WrongTypeException;

public final class RedisReliableConsumer<T> {

    private RedisReliableMover mover;
    private IRedisReliableMover<T> imover;
    private RedisReliableDeleter deleter;
    private IRedisReliableDeleter<T> ideleter;

    public RedisReliableConsumer(IRedisClient client, IRedisReliableMover<T> imover, IRedisReliableDeleter<T> ideleter) {
        this.mover = new RedisReliableMover(client);
        this.imover = imover;
        this.deleter = new RedisReliableDeleter(client);
        this.ideleter = ideleter;
    }

    public T consume(final String key, final String inprocesskey) throws WrongTypeException {
        return consume(key, inprocesskey, null);
    }

    public T consume(final String key, final String inprocesskey, final T element) throws WrongTypeException {
        return mover.move(imover, key, inprocesskey, element);
    }

    public T ack(final String key, final String inprocesskey, final T element) throws WrongTypeException {
        return deleter.delete(ideleter, inprocesskey, element);
    }

    public T fail(final String key, final String inprocesskey, final T element) throws WrongTypeException {
        return mover.move(imover, inprocesskey, key, element);
    }

    public static RedisReliableConsumer<String> StringConsumer(IRedisClient client) {
        return new RedisReliableConsumer<String>(client, new RedisReliableStringMover(), new RedisReliableStringDeleter());
    }

    public static RedisReliableConsumer<String> ListConsumer(IRedisClient client) {
        return new RedisReliableConsumer<String>(client, new RedisReliableListMover(true), new RedisReliableListDeleter());
    }

    public static RedisReliableConsumer<String> SetConsumer(IRedisClient client) {
        return new RedisReliableConsumer<String>(client, new RedisReliableSetMover(), new RedisReliableSetDeleter());
    }

}