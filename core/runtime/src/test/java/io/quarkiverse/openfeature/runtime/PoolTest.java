package io.quarkiverse.openfeature.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

// depends on per-method test lifecycle!
public class PoolTest {
    private static final Duration MAX_WAIT = Duration.ofMillis(10);
    // long enough that no instance can be evicted during a test
    private static final Duration NEVER_IDLE_TIMEOUT = Duration.ofHours(1);
    // short enough that every idle instance is immediately eligible for eviction
    private static final Duration IMMEDIATE_IDLE_TIMEOUT = Duration.ZERO;

    private final Instances instances = new Instances();

    static class Instance {
        final int id;
        volatile boolean destroyed;

        Instance(int id) {
            this.id = id;
        }
    }

    static class Instances implements Supplier<Instance>, Consumer<Instance> {
        final AtomicInteger created = new AtomicInteger();
        final List<Instance> destroyed = Collections.synchronizedList(new ArrayList<>());

        @Override
        public Instance get() {
            return new Instance(created.incrementAndGet());
        }

        @Override
        public void accept(Instance instance) {
            instance.destroyed = true;
            destroyed.add(instance);
        }
    }

    private Pool<Instance> pool(int minSize, Duration idleTimeout) {
        return pool(minSize, idleTimeout, idleTimeout);
    }

    private Pool<Instance> pool(int minSize, Duration fastIdleTimeout, Duration slowIdleTimeout) {
        return pool(instances, minSize, fastIdleTimeout, slowIdleTimeout);
    }

    private Pool<Instance> pool(Supplier<Instance> factory, int minSize, Duration fastIdleTimeout,
            Duration slowIdleTimeout) {
        return new Pool<>("test instance", minSize, MAX_WAIT, fastIdleTimeout, slowIdleTimeout,
                factory, instances);
    }

    private static List<Instance> borrowAll(Pool<Instance> pool, int count) {
        List<Instance> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(pool.borrow());
        }
        return result;
    }

    private static void releaseAll(Pool<Instance> pool, List<Instance> instances) {
        for (Instance instance : instances) {
            pool.release(instance);
        }
    }

    @Test
    public void minimumSizeMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> pool(0, NEVER_IDLE_TIMEOUT));
        assertEquals(0, instances.created.get());
    }

    @Test
    public void createsMinimumNumberOfInstancesEagerly() {
        Pool<Instance> pool = pool(3, NEVER_IDLE_TIMEOUT);

        assertEquals(3, instances.created.get());
        assertEquals(3, pool.size());
    }

    @Test
    public void reusesReleasedInstance() {
        Pool<Instance> pool = pool(2, NEVER_IDLE_TIMEOUT);

        Instance instance = pool.borrow();
        pool.release(instance);

        assertSame(instance, pool.borrow());
        assertEquals(2, instances.created.get());
    }

    @Test
    public void withInstanceReleasesOnSuccessAndOnFailure() {
        Pool<Instance> pool = pool(1, NEVER_IDLE_TIMEOUT);

        int id = pool.withInstance(instance -> instance.id);
        assertEquals(1, id);
        assertThrows(IllegalStateException.class, () -> pool.withInstance(instance -> {
            throw new IllegalStateException("boom");
        }));

        // the instance was released both times, so it can be borrowed again without growing
        id = pool.withInstance(instance -> instance.id);
        assertEquals(1, id);
        assertEquals(1, instances.created.get());
    }

    @Test
    public void growsUpToFourTimesTheMinimumSize() {
        Pool<Instance> pool = pool(2, NEVER_IDLE_TIMEOUT);

        List<Instance> borrowed = borrowAll(pool, 8);

        assertEquals(8, instances.created.get());
        assertEquals(8, pool.size());
        assertEquals(8, borrowed.stream().distinct().count());
    }

    @Test
    public void timesOutWhenSaturated() {
        Pool<Instance> pool = pool(1, NEVER_IDLE_TIMEOUT);
        borrowAll(pool, 4);

        long start = System.nanoTime();
        IllegalStateException e = assertThrows(IllegalStateException.class, pool::borrow);
        long elapsed = System.nanoTime() - start;

        assertTrue(e.getMessage().contains("Timed out"), e.getMessage());
        assertTrue(elapsed >= MAX_WAIT.toNanos(), "should wait at least " + MAX_WAIT);
        // generous upper bound, only to catch waiting for far too long
        assertTrue(elapsed < TimeUnit.SECONDS.toNanos(5), "should not wait much longer than " + MAX_WAIT);
        assertEquals(4, pool.size());
    }

    @Test
    public void waitsForInstanceReleasedByAnotherThread() throws Exception {
        Pool<Instance> pool = pool(1, NEVER_IDLE_TIMEOUT);
        List<Instance> borrowed = borrowAll(pool, 4);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch borrowing = new CountDownLatch(1);
            Future<Instance> borrow = executor.submit(() -> {
                borrowing.countDown();
                return pool.borrow();
            });
            assertTrue(borrowing.await(5, TimeUnit.SECONDS));
            pool.release(borrowed.get(0));

            assertSame(borrowed.get(0), borrow.get(5, TimeUnit.SECONDS));
            assertEquals(4, instances.created.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void interruptedWhileWaiting() throws Exception {
        Pool<Instance> pool = pool(1, NEVER_IDLE_TIMEOUT);
        borrowAll(pool, 4);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                pool.borrow();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        thread.start();
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(5));

        assertInstanceOf(IllegalStateException.class, failure.get());
        assertTrue(failure.get().getMessage().contains("Interrupted"), failure.get().getMessage());
        assertInstanceOf(InterruptedException.class, failure.get().getCause());
    }

    @Test
    public void shrinksBackToMinimumSizeWhenInstancesAreIdle() {
        Pool<Instance> pool = pool(2, IMMEDIATE_IDLE_TIMEOUT);

        List<Instance> borrowed = borrowAll(pool, 8);
        assertEquals(8, pool.size());
        releaseAll(pool, borrowed);

        assertEquals(2, pool.size());
        assertEquals(6, instances.destroyed.size());
        // the instances that are kept are the most recently released ones
        List<Instance> kept = new ArrayList<>(borrowed);
        kept.removeAll(instances.destroyed);
        assertEquals(List.of(borrowed.get(6), borrowed.get(7)), kept);
    }

    @Test
    public void shrinksPeakCapacityQuicklyAndTheRestSlowly() {
        Pool<Instance> pool = pool(2, IMMEDIATE_IDLE_TIMEOUT, NEVER_IDLE_TIMEOUT);

        releaseAll(pool, borrowAll(pool, 8));

        // the peak capacity above 2 * minimum size is gone, the rest is kept for much longer
        assertEquals(4, pool.size());
        assertEquals(4, instances.destroyed.size());
    }

    @Test
    public void doesNotShrinkBeforeTheIdleTimeout() {
        Pool<Instance> pool = pool(1, NEVER_IDLE_TIMEOUT);

        releaseAll(pool, borrowAll(pool, 4));

        assertEquals(4, pool.size());
        assertEquals(0, instances.destroyed.size());
    }

    @Test
    public void slotIsReleasedWhenInstanceCreationFails() {
        Supplier<Instance> failingFactory = () -> {
            Instance instance = instances.get();
            if (instance.id == 2) {
                throw new IllegalStateException("cannot create");
            }
            return instance;
        };
        Pool<Instance> pool = pool(failingFactory, 1, NEVER_IDLE_TIMEOUT, NEVER_IDLE_TIMEOUT);

        pool.borrow();
        assertThrows(IllegalStateException.class, pool::borrow);
        assertEquals(1, pool.size());

        // the failed attempt did not consume a slot, the pool can still grow
        assertNotNull(pool.borrow());
        assertEquals(2, pool.size());
    }

    @Test
    public void closeDestroysIdleInstances() {
        Pool<Instance> pool = pool(3, NEVER_IDLE_TIMEOUT);

        pool.close();

        assertEquals(3, instances.destroyed.size());
        assertEquals(0, pool.size());
        assertThrows(IllegalStateException.class, pool::borrow);
    }

    @Test
    public void closeDestroysBorrowedInstancesWhenTheyAreReleased() {
        Pool<Instance> pool = pool(2, NEVER_IDLE_TIMEOUT);

        Instance borrowed = pool.borrow();
        pool.close();

        assertEquals(1, instances.destroyed.size());
        assertFalse(borrowed.destroyed);

        pool.release(borrowed);

        assertEquals(2, instances.destroyed.size());
        assertTrue(borrowed.destroyed);
        assertEquals(0, pool.size());
    }

    @Test
    public void concurrentBorrowAndReleaseNeverExceedsTheMaximumSize() throws Exception {
        int minSize = 4;
        int maxSize = Pool.MAX_FACTOR * minSize;
        Pool<Instance> pool = pool(minSize, NEVER_IDLE_TIMEOUT);

        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int j = 0; j < 500; j++) {
                        Instance instance = pool.withInstance(borrowed -> borrowed);
                        assertFalse(instance.destroyed);
                        assertTrue(pool.size() <= maxSize);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertTrue(instances.created.get() <= maxSize, "created " + instances.created.get() + " instances");
        assertTrue(pool.size() <= maxSize);
        assertTrue(pool.size() >= minSize);
    }
}
