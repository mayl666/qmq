package qunar.tc.qmq.common;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yiqun.fan create on 17-8-28.
 */
public class LimitedBlockingQueueWrapper<E> implements BlockingQueue<E> {
    private final LinkedBlockingQueue<E> queue = new LinkedBlockingQueue<>();
    private final AtomicInteger limit = new AtomicInteger(Integer.MAX_VALUE);

    public LimitedBlockingQueueWrapper(int initLimit) {
        setLimit(initLimit);
    }

    public int getLimit() {
        return limit.get();
    }

    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "limit must be greater than 0");
        this.limit.set(limit);
    }

    @Override
    public boolean add(E e) {
        if (limit.get() > queue.size())
            return queue.add(e);
        throw new IllegalStateException("Queue full");
    }

    @Override
    public boolean offer(E e) {
        return limit.get() > queue.size() && queue.offer(e);
    }

    @Override
    public void put(E e) throws InterruptedException {
        queue.put(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(e, timeout, unit);
    }

    @Override
    public E take() throws InterruptedException {
        return queue.take();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return limit.get() - queue.size();
    }

    @Override
    public boolean remove(Object o) {
        return queue.remove(o);
    }

    @Override
    public boolean contains(Object o) {
        return queue.contains(o);
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return queue.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return queue.drainTo(c, maxElements);
    }

    @Override
    public E remove() {
        return queue.remove();
    }

    @Override
    public E poll() {
        return queue.poll();
    }

    @Override
    public E element() {
        return queue.element();
    }

    @Override
    public E peek() {
        return queue.peek();
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return queue.iterator();
    }

    @Override
    public Object[] toArray() {
        return queue.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return queue.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return queue.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        boolean modified = false;
        for (E e : c)
            if (add(e))
                modified = true;
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return queue.retainAll(c);
    }

    @Override
    public void clear() {
        queue.clear();
    }
}
