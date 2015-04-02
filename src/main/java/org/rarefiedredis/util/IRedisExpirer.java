package org.rarefiedredis.util;

public interface IRedisExpirer {

    public void expired(String key, String element);

    public void persisted(String key, String element);

    public void exists(String key, String element);

    public void doesNotExist(String key, String element);

    public void expireError(Exception e);

    public void persistError(Exception e);

    public void checkError(Exception e);
    
    public void existsError(Exception e);

}
