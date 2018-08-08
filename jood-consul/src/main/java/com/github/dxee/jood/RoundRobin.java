package com.github.dxee.jood;

import java.util.Iterator;
import java.util.List;

/**
 * Round robin
 *
 * @author bing.fan
 * 2018-08-02 14:50
 */
public class RoundRobin<T> {

    private Iterator<Robin<T>> it;
    private List<Robin<T>> list;

    public RoundRobin(List<Robin<T>> list) {
        this.list = list;
        it = list.iterator();
    }

    public T next() {
        // if we get to the end, start again
        if (!it.hasNext()) {
            it = list.iterator();
        }
        Robin<T> robin = it.next();

        return robin.call();
    }

    public static class Robin<T> {
        private T obj;

        public Robin(T obj) {
            this.obj = obj;
        }

        public T call() {
            return obj;
        }
    }
}
