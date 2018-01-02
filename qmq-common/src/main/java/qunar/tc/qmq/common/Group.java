package qunar.tc.qmq.common;

import com.google.common.base.Function;

import java.util.*;

/**
 * Created by zhaohui.yu
 * 15/11/2
 */
public class Group {
    public static <Key, T> Map<Key, List<T>> by(Collection<T> items, Function<T, Key> func) {
        Map<Key, List<T>> result = new HashMap<Key, List<T>>();
        for (T item : items) {
            Key key = func.apply(item);
            List<T> list = result.get(key);
            if (list == null) {
                list = new ArrayList<T>();
                result.put(key, list);
            }
            list.add(item);
        }
        return result;
    }
}
