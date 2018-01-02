package qunar.tc.qmq.concurrent;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.NamedThreadFactory;
import qunar.metrics.Gauge;
import qunar.metrics.Metrics;

import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaohui.yu
 * 16/4/8
 */
public class ActorSystem {
    private static final Logger logger = LoggerFactory.getLogger(ActorSystem.class);

    private static final int DEFAULT_QUEUE_SIZE = 10000;

    private final ConcurrentMap<String, Actor> actors;
    private final ExecutorService executor;
    private final AtomicInteger actorsCount;
    private final String name;

    public ActorSystem(String name) {
        this.name = name;
        this.actorsCount = new AtomicInteger();
        int cores = Runtime.getRuntime().availableProcessors() * 2;
        this.executor = new ThreadPoolExecutor(cores, cores, 60, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("actor-sys-" + name));
        this.actors = Maps.newConcurrentMap();
        Metrics.gauge("dispatchers-" + name).keep().call(new Gauge() {
            @Override
            public double getValue() {
                return actorsCount.get();
            }
        });
    }

    public <E> void dispatch(String actorPath, E msg, Processor<E> processor) {
        Actor<E> actor = createOrGet(actorPath, processor);
        actor.dispatch(msg);
        schedule(actor, true);
    }

    public void suspend(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.suspend();
    }

    public void resume(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.resume();
    }

    //TODO
    private <E> Actor<E> createOrGet(String actorPath, Processor<E> processor) {
        Actor<E> actor = actors.get(actorPath);
        if (actor != null) return actor;

        Actor<E> add = new Actor<>(this.name, actorPath, this, processor, DEFAULT_QUEUE_SIZE);
        Actor<E> old = actors.putIfAbsent(actorPath, add);
        if (old == null) {
            logger.info("create actorSystem: {}", actorPath);
            actorsCount.incrementAndGet();
            return add;
        }
        return old;
    }

    private <E> boolean schedule(Actor<E> actor, boolean hasMessageHint) {
        if (!actor.canBeSchedule(hasMessageHint)) return false;
        if (actor.setAsRunnable()) {
            this.executor.execute(actor);
            return true;
        }
        return false;
    }

    static class Actor<E> implements Runnable {

        //actor可被调度
        private static final int RUNNABLE = 0;
        //actor进入调度队列
        private static final int QUEUE = 1;
        //actor正在运行
        private static final int RUNNING = 2;
        //actor暂停
        private static final int SUSPEND = 3;
        //actor停止
        private static final int DEAD = 4;

        private final AtomicInteger status = new AtomicInteger(RUNNABLE);

        final String systemName;
        protected final String name;
        final ActorSystem actorSystem;
        final BoundedNodeQueue<E> queue;
        final Processor<E> processor;

        Actor(String systemName, String name, ActorSystem actorSystem, Processor<E> processor, int queueSize) {
            this.systemName = systemName;
            this.name = name;
            this.actorSystem = actorSystem;
            this.processor = processor;
            this.queue = new BoundedNodeQueue<>(queueSize);
        }

        boolean dispatch(E message) {
            return this.queue.add(message);
        }

        void processMessages() {
            E message = queue.poll();
            if (message == null) return;
            processor.process(message);
        }

        boolean setAsRunnable() {
            return status.compareAndSet(RUNNABLE, QUEUE);
        }

        boolean setAsIdle() {
            return status.compareAndSet(RUNNING, RUNNABLE);
        }

        private boolean setAsRunning() {
            return status.compareAndSet(QUEUE, RUNNING);
        }

        void suspend() {
            if (status.get() == DEAD) return;
            status.set(SUSPEND);
        }

        void resume() {
            if (status.get() == DEAD) return;
            if (status.get() != SUSPEND) return;

            if (status.compareAndSet(SUSPEND, RUNNABLE)) {
                actorSystem.schedule(this, false);
            }
        }

        private boolean canBeSchedule(boolean hasMessageHint) {
            return hasMessageHint || !queue.isEmpty();
        }

        @Override
        public void run() {
            if (!setAsRunning()) return;
            String old = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(systemName + "-" + name);
                processMessages();
            } finally {
                Thread.currentThread().setName(old);
                if (setAsIdle()) {
                    this.actorSystem.schedule(this, false);
                }
            }
        }
    }

    public interface Processor<T> {
        void process(T message);
    }

    /**
     * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
     */

    /**
     * Lock-free bounded non-blocking multiple-producer single-consumer queue based on the works of:
     * <p/>
     * Andriy Plokhotnuyk (https://github.com/plokhotnyuk)
     * - https://github.com/plokhotnyuk/actors/blob/2e65abb7ce4cbfcb1b29c98ee99303d6ced6b01f/src/test/scala/akka/dispatch/Mailboxes.scala
     * (Apache V2: https://github.com/plokhotnyuk/actors/blob/master/LICENSE)
     * <p/>
     * Dmitriy Vyukov's non-intrusive MPSC queue:
     * - http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
     * (Simplified BSD)
     */
    @SuppressWarnings("serial")
    private static class BoundedNodeQueue<T> {

        private final int capacity;

        @SuppressWarnings("unused")
        private volatile Node<T> _enqDoNotCallMeDirectly;

        @SuppressWarnings("unused")
        private volatile Node<T> _deqDoNotCallMeDirectly;

        protected BoundedNodeQueue(final int capacity) {
            if (capacity < 0) throw new IllegalArgumentException("AbstractBoundedNodeQueue.capacity must be >= 0");
            this.capacity = capacity;
            final Node<T> n = new Node<T>();
            setDeq(n);
            setEnq(n);
        }

        private void setEnq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, enqOffset, n);
        }

        @SuppressWarnings("unchecked")
        private Node<T> getEnq() {
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, enqOffset);
        }

        private boolean casEnq(Node<T> old, Node<T> nju) {
            return Unsafe.instance.compareAndSwapObject(this, enqOffset, old, nju);
        }

        private void setDeq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, deqOffset, n);
        }

        @SuppressWarnings("unchecked")
        private Node<T> getDeq() {
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, deqOffset);
        }

        private boolean casDeq(Node<T> old, Node<T> nju) {
            return Unsafe.instance.compareAndSwapObject(this, deqOffset, old, nju);
        }

        /**
         * @return the maximum capacity of this queue
         */
        public final int capacity() {
            return capacity;
        }

        // Possible TODO — impl. could be switched to addNode(new Node(value)) if we want to allocate even if full already
        public final boolean add(final T value) {
            for (Node<T> n = null; ; ) {
                final Node<T> lastNode = getEnq();
                final int lastNodeCount = lastNode.count;
                if (lastNodeCount - getDeq().count < capacity) {
                    // Trade a branch for avoiding to create a new node if full,
                    // and to avoid creating multiple nodes on write conflict á la Be Kind to Your GC
                    if (n == null) {
                        n = new Node<T>();
                        n.value = value;
                    }

                    n.count = lastNodeCount + 1; // Piggyback on the HB-edge between getEnq() and casEnq()

                    // Try to putPullLogs the node to the end, if we fail we continue loopin'
                    if (casEnq(lastNode, n)) {
                        lastNode.setNext(n);
                        return true;
                    }
                } else return false; // Over capacity—couldn't add the node
            }
        }

        public final boolean isEmpty() {
            return getEnq() == getDeq();
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the value of the first element of the queue, null if empty
         */
        public final T poll() {
            final Node<T> n = pollNode();
            return (n != null) ? n.value : null;
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the `Node` of the first element of the queue, null if empty
         */
        public final Node<T> pollNode() {
            for (; ; ) {
                final Node<T> deq = getDeq();
                final Node<T> next = deq.next();
                if (next != null) {
                    if (casDeq(deq, next)) {
                        deq.value = next.value;
                        deq.setNext(null);
                        next.value = null;
                        return deq;
                    } // else we retry (concurrent consumers)
                } else if (getEnq() == deq) return null; // If we got a null and head meets tail, we are empty
            }
        }

        private final static long enqOffset, deqOffset;

        static {
            try {
                enqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_enqDoNotCallMeDirectly"));
                deqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_deqDoNotCallMeDirectly"));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        public static class Node<T> {
            protected T value;
            @SuppressWarnings("unused")
            private volatile Node<T> _nextDoNotCallMeDirectly;
            protected int count;

            @SuppressWarnings("unchecked")
            public final Node<T> next() {
                return (Node<T>) Unsafe.instance.getObjectVolatile(this, nextOffset);
            }

            protected final void setNext(final Node<T> newNext) {
                Unsafe.instance.putOrderedObject(this, nextOffset, newNext);
            }

            private final static long nextOffset;

            static {
                try {
                    nextOffset = Unsafe.instance.objectFieldOffset(Node.class.getDeclaredField("_nextDoNotCallMeDirectly"));
                } catch (Throwable t) {
                    throw new ExceptionInInitializerError(t);
                }
            }
        }
    }

    static class Unsafe {
        public final static sun.misc.Unsafe instance;

        static {
            try {
                sun.misc.Unsafe found = null;
                for (Field field : sun.misc.Unsafe.class.getDeclaredFields()) {
                    if (field.getType() == sun.misc.Unsafe.class) {
                        field.setAccessible(true);
                        found = (sun.misc.Unsafe) field.get(null);
                        break;
                    }
                }
                if (found == null) throw new IllegalStateException("Can't find instance of sun.misc.Unsafe");
                else instance = found;
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }
    }
}
