package org.rarefiedredis.adapter;

import org.rarefiedredis.redis.IRedisClient;
import org.rarefiedredis.redis.NoKeyException;
import org.rarefiedredis.redis.WrongTypeException;
import org.rarefiedredis.redis.NotImplementedException;
import org.rarefiedredis.redis.IndexOutOfRangeException;

import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Collection;

public final class RedisListAdapter implements List<String> {

    private IRedisClient client;
    private String key;

    public RedisListAdapter(IRedisClient client, String key) {
        this.client = client;
        this.key = key;
    }

    @Override public boolean add(String e) throws IllegalArgumentException, UnsupportedOperationException {
        try {
            client.rpush(key, e);
        }
        catch (WrongTypeException x) {
            throw new IllegalArgumentException();
        }
        catch (NotImplementedException x) {
            throw new UnsupportedOperationException();
        }
        return true;
    }

    @Override public void add(int index, String e) throws NullPointerException, IndexOutOfBoundsException, IllegalArgumentException, UnsupportedOperationException {
        if (e == null) {
            throw new NullPointerException();
        }
        try {
            client.lset(key, (long)index, e);
            return;
        }
        catch (IndexOutOfRangeException x) {
            throw new IndexOutOfBoundsException();
        }
        catch (NoKeyException x) {
            throw new IllegalArgumentException();
        }
        catch (WrongTypeException x) {
            throw new IllegalArgumentException();
        }
        catch (NotImplementedException x) {
            throw new UnsupportedOperationException();
        }
    }

    @Override public boolean addAll(Collection<? extends String> c) {
        Iterator<? extends String> iter = c.iterator();
        while (iter.hasNext()) {
            try {
                if (!add((String)iter.next())) {
                    return false;
                }
            }
            catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    @Override public boolean addAll(int index, Collection<? extends String> c) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override public void clear() throws UnsupportedOperationException {
        try {
            client.del(key);
        }
        catch (NotImplementedException e) {
            throw new UnsupportedOperationException();
        }
    }

    @Override public boolean contains(Object o) throws NullPointerException, ClassCastException {
        if (o == null) {
            throw new NullPointerException();
        }
        if (!(o instanceof String)) {
            throw new ClassCastException();
        }
        String element = (String)o;
        try {
            Long atATime = 10L;
            Long start = 0L;
            Long stop = start + atATime - 1L; 
            while (true) {
                List<String> range = client.lrange(key, start, stop);
                if (range.isEmpty()) {
                    break;
                }
                if (range.contains(element)) {
                    return true;
                }
                start = stop + 1L;
                stop = start + atATime - 1L;
            }
        }
        catch (WrongTypeException e) {
            return false;
        }
        catch (NotImplementedException e) {
            return false;
        }
        return false;
    }

    @Override public boolean containsAll(Collection<?> c) {
        Iterator<?> iter = c.iterator();
        while (iter.hasNext()) {
            Object next = iter.next();
            try {
                if (!contains(next)) {
                    return false;
                }
            }
            catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    @Override public String get(int index) {
        try { 
            return client.lindex(key, (long)index);
        }
        catch (WrongTypeException e) {
            return null;
        }
        catch (NotImplementedException e) {
            return null;
        }
    }

    @Override public int indexOf(Object o) throws NullPointerException, ClassCastException {
        if (o == null) {
            throw new NullPointerException();
        }
        if (!(o instanceof String)) {
            throw new ClassCastException();
        }
        String element = (String)o;
        Long index = 0L;
        try {
            Long atATime = 10L;
            Long start = 0L;
            Long stop = start + atATime - 1L; 
            while (true) {
                List<String> range = client.lrange(key, start, stop);
                if (range.isEmpty()) {
                    break;
                }
                if (range.indexOf(element) != -1) {
                    return (int)(index + (long)range.indexOf(element));
                }
                start = stop + 1L;
                stop = start + atATime - 1L;
            }
        }
        catch (WrongTypeException e) {
            return -1;
        }
        catch (NotImplementedException e) {
            return -1;
        }
        return -1;
    }

    @Override public boolean isEmpty() {
        try {
            return (long)client.llen(key) == 0L;
        }
        catch (WrongTypeException e) {
            return true;
        }
        catch (NotImplementedException e) {
            return true;
        }
    }

    @Override public Iterator<String> iterator() {
        return new Iterator<String>() {
            private Long index = 0L;
            @Override public boolean hasNext() {
                try {
                    return index < client.llen(key);
                }
                catch (Exception e) {
                    return false;
                }
            }
            @Override public String next() {
                try {
                    return client.lindex(key, index++);
                }
                catch (Exception e) {
                    return null;
                }
            }
            @Override public void remove() throws UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override public int lastIndexOf(Object o) throws NullPointerException, ClassCastException {
        if (o == null) {
            throw new NullPointerException();
        }
        if (!(o instanceof String)) {
            throw new ClassCastException();
        }
        String element = (String)o;
        Long index = 0L;
        try {
            Long atATime = 10L;
            Long start = 0L;
            Long stop = start + atATime - 1L; 
            while (true) {
                List<String> range = client.lrange(key, start, stop);
                if (range.isEmpty()) {
                    break;
                }
                if (range.indexOf(element) != -1) {
                    index = index + (long)range.indexOf(element);
                }
                start = stop + 1L;
                stop = start + atATime - 1L;
            }
        }
        catch (WrongTypeException e) {
            return -1;
        }
        catch (NotImplementedException e) {
            return -1;
        }
        return index.intValue();
    }

    @Override public ListIterator<String> listIterator() {
        return null;
    }

    @Override public ListIterator<String> listIterator(int index) {
        return null;
    }

    @Override public String remove(int index) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object o) throws NullPointerException, ClassCastException {
        if (o == null) {
            throw new NullPointerException();
        }
        if (!(o instanceof String)) {
            throw new ClassCastException();
        }
        String element = (String)o;
        try {
            Long llen = client.llen(key);
            client.lrem(key, 1L, element);
            return llen > client.llen(key);
        }
        catch (Exception e) {
            return false;
        }
    }

    @Override public boolean removeAll(Collection<?> c) {
        boolean changed = false;
        Iterator<?> iter = c.iterator();
        while (iter.hasNext()) {
            Object o = iter.next();
            try {
                if (remove(o)) {
                    changed = true;
                }
            }
            catch (Exception e) {
                return false;
            }
        }
        return changed;
    }

    @Override public boolean retainAll(Collection<?> c) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override public String set(int index, String element) throws NullPointerException, IndexOutOfBoundsException, IllegalArgumentException, UnsupportedOperationException {
        if (element == null) {
            throw new NullPointerException();
        }
        try {
            String previous = client.lindex(key, (long)index);
            client.lset(key, (long)index, element);
            return previous;
        }
        catch (IndexOutOfRangeException e) {
            throw new IndexOutOfBoundsException();
        }
        catch (NoKeyException e) {
            throw new IllegalArgumentException();
        }
        catch (WrongTypeException e) {
            throw new IllegalArgumentException();
        }
        catch (NotImplementedException e) {
            throw new UnsupportedOperationException();
        }
    }

    @Override public int size() {
        try {
            return client.llen(key).intValue();
        }
        catch (WrongTypeException e) {
            return 0;
        }
        catch (NotImplementedException e) {
            return 0;
        }
    }

    @Override public List<String> subList(int fromIndex, int toIndex) {
        try {
            return client.lrange(key, (long)fromIndex, (long)(toIndex + 1));
        }
        catch (WrongTypeException e) {
            return null;
        }
        catch (NotImplementedException e) {
            return null;
        }
    }

    @Override public Object[] toArray() {
        Long llen;
        try {
            llen = client.llen(key);
        }
        catch (Exception e) {
            return null;
        }
        Object[] array = new Object[llen.intValue()];
        int index = 0;
        try {
            Long atATime = 10L;
            Long start = 0L;
            Long stop = start + atATime - 1L;
            while (true) {
                List<String> range = client.lrange(key, start, stop);
                if (range.isEmpty()) {
                    break;
                }
                for (int idx = 0; idx < range.size(); ++idx, ++index) {
                    array[index] = range.get(idx);
                }
                start = stop + 1L;
                stop = start + atATime - 1L;
            }
        }
        catch (WrongTypeException e) {
            return null;
        }
        catch (NotImplementedException e) {
            return null;
        }
        return array;
    }

    @Override public <T> T[] toArray(T[] a) {
        return null;
    }

}