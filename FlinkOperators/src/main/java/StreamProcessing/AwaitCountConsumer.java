package StreamProcessing;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AwaitCountConsumer implements Consumer<Object> {
    private final AtomicInteger counter = new AtomicInteger();
    private final int expectedSize;

    public AwaitCountConsumer(int expectedSize) {
        this.expectedSize = expectedSize;
    }

    @Override
    public void accept(Object o) {
        if (counter.incrementAndGet() == expectedSize) {
            synchronized (counter) {
                counter.notifyAll();
            }
        }
    }

    public long got() {
        return counter.get();
    }

    public void await(long timeout, TimeUnit unit) throws InterruptedException {
        final long stop = System.currentTimeMillis() + unit.toMillis(timeout);
        synchronized (counter) {
            while (counter.longValue() < expectedSize && System.currentTimeMillis() < stop) {
                counter.wait(unit.toMillis(timeout));
            }
        }
    }
}
