package io.quarkiverse.openfeature.runtime;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

/**
 * A pool of instances that are expensive to create and may only be used by one thread at a time,
 * such as WASM evaluation engines.
 * <p>
 * The pool always holds at least {@code minSize} instances, which are created eagerly in
 * the constructor, and grows on demand up to {@value MAX_FACTOR} * {@code minSize} instances. Only
 * when the pool is full does borrowing wait for an instance to be released, and only for a short
 * while ({@value #MAX_WAIT_MILLIS} ms) before failing.
 * <p>
 * Instances created above the minimum size are destroyed again when they become idle. While
 * the pool is above {@value #PEAK_FACTOR} * {@code minSize} instances, idle instances are
 * destroyed after {@value #FAST_IDLE_TIMEOUT_MINUTES} minute(s). The last {@code minSize}
 * instances above the minimum size are destroyed after {@value #SLOW_IDLE_TIMEOUT_MINUTES}
 * minutes, so that a pool under continuous moderate load doesn't keep creating and destroying
 * instances.
 * <p>
 * Shrinking happens as a side effect of borrowing and releasing instances, so a pool that grew
 * and then went completely idle keeps the extra instances until it is used again.
 */
public final class Pool<T> {
    private static final Logger log = Logger.getLogger(Pool.class);

    static final int PEAK_FACTOR = 2;
    static final int MAX_FACTOR = 4;

    static final int MAX_WAIT_MILLIS = 10;
    static final int FAST_IDLE_TIMEOUT_MINUTES = 1;
    static final int SLOW_IDLE_TIMEOUT_MINUTES = 10;

    private final String description;
    private final int minSize;
    // instances above this size are destroyed quickly when idle, instances between
    // this size and the minimum size are destroyed slowly
    private final int peakSize;
    private final int maxSize;
    private final long maxWaitNanos;
    private final long fastIdleTimeoutNanos;
    private final long slowIdleTimeoutNanos;
    private final Supplier<T> creator;
    private final Consumer<T> destroyer;

    // one permit per instance that may be borrowed, so the pool never exceeds the maximum size
    private final Semaphore permits;
    // LIFO: the most recently released instance is first, so the coldest instance is last
    private final ConcurrentLinkedDeque<Idle<T>> idle = new ConcurrentLinkedDeque<>();
    // number of instances that exist, both idle and currently borrowed
    private final AtomicInteger total = new AtomicInteger();
    private volatile boolean closed;

    /**
     * Creates a pool of {@code minSize} instances that may grow up to {@code 4 * minSize}.
     * The {@code description} is used in log and error messages, it should name a single
     * instance (such as {@code "WASM engine"}).
     */
    public Pool(String description, int minSize, Supplier<T> creator, Consumer<T> destroyer) {
        this(description, minSize, Duration.ofMillis(MAX_WAIT_MILLIS), Duration.ofMinutes(FAST_IDLE_TIMEOUT_MINUTES),
                Duration.ofMinutes(SLOW_IDLE_TIMEOUT_MINUTES), creator, destroyer);
    }

    // visible for testing
    Pool(String description, int minSize, Duration maxWait, Duration fastIdleTimeout, Duration slowIdleTimeout,
            Supplier<T> creator, Consumer<T> destroyer) {
        if (minSize < 1) {
            throw new IllegalArgumentException("Pool size of '" + description + "' must be at least 1");
        }
        this.description = description;
        this.minSize = minSize;
        this.peakSize = PEAK_FACTOR * minSize;
        this.maxSize = MAX_FACTOR * minSize;
        this.maxWaitNanos = maxWait.toNanos();
        this.fastIdleTimeoutNanos = fastIdleTimeout.toNanos();
        this.slowIdleTimeoutNanos = slowIdleTimeout.toNanos();
        this.creator = creator;
        this.destroyer = destroyer;
        this.permits = new Semaphore(maxSize);

        long now = System.nanoTime();
        for (int i = 0; i < minSize; i++) {
            idle.addLast(new Idle<>(creator.get(), now));
        }
        total.set(minSize);
    }

    /**
     * Borrows an instance from the pool, passes it to {@code action} and returns the instance
     * to the pool afterwards. The instance must not be used after {@code action} returns;
     * ideally, the {@code action} doesn't store the instance anywhere and only uses it locally.
     */
    public <R> R withInstance(Function<T, R> action) {
        T instance = borrow();
        try {
            return action.apply(instance);
        } finally {
            release(instance);
        }
    }

    /**
     * Destroys all instances in the pool. Instances that are currently borrowed are destroyed
     * when they are released.
     */
    public void close() {
        closed = true;
        destroyIdle();
    }

    // visible for testing
    T borrow() {
        if (closed) {
            throw new IllegalStateException("Pool of '" + description + "' instances is closed");
        }

        // a permit is the right to hold an instance, so there are never more instances
        // than the maximum size; when the pool is saturated, the waiting thread is parked
        // by the semaphore and woken up by whoever releases an instance
        try {
            if (!permits.tryAcquire(maxWaitNanos, TimeUnit.NANOSECONDS)) {
                log.errorf("Timed out waiting for a '%s' instance, all %d instances are in use",
                        description, maxSize);
                throw new IllegalStateException("Timed out waiting for a '" + description + "' instance");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a '" + description + "' instance", e);
        }

        try {
            Idle<T> instance = idle.pollFirst();
            if (instance != null) {
                evictIdle();
                return instance.instance;
            }
            return create();
        } catch (RuntimeException | Error e) {
            permits.release();
            throw e;
        }
    }

    // visible for testing
    void release(T instance) {
        // the instance must be in the deque before the permit is released,
        // otherwise a woken up thread could find no instance and create one
        idle.addFirst(new Idle<>(instance, System.nanoTime()));
        permits.release();
        if (closed) {
            // the pool was closed while the instance was borrowed
            destroyIdle();
            return;
        }
        evictIdle();
    }

    // visible for testing
    int size() {
        return total.get();
    }

    private T create() {
        log.debugf("Growing the pool of '%s' instances", description);
        T instance = creator.get();
        total.incrementAndGet();
        return instance;
    }

    private void evictIdle() {
        log.debugf("Shrinking the pool of '%s' instances", description);
        while (true) {
            int total = this.total.get();
            if (total <= minSize) {
                return;
            }
            long idleTimeoutNanos = total > peakSize ? fastIdleTimeoutNanos : slowIdleTimeoutNanos;
            Idle<T> coldest = idle.peekLast();
            if (coldest == null || System.nanoTime() - coldest.idleSince < idleTimeoutNanos) {
                return;
            }
            // reserve the removal first, so that the pool can never shrink below the minimum size
            if (!this.total.compareAndSet(total, total - 1)) {
                continue;
            }
            // not necessarily the instance seen above, but that only matters for eviction order
            Idle<T> evicted = idle.pollLast();
            if (evicted == null) {
                this.total.incrementAndGet();
                return;
            }
            destroyer.accept(evicted.instance);
        }
    }

    private void destroyIdle() {
        Idle<T> instance;
        while ((instance = idle.pollFirst()) != null) {
            total.decrementAndGet();
            destroyer.accept(instance.instance);
        }
    }

    private record Idle<T>(T instance, long idleSince) {
    }
}
