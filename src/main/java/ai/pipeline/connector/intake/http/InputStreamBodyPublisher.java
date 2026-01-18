package ai.pipeline.connector.intake.http;

import io.smallrye.mutiny.infrastructure.Infrastructure;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Flow;

/**
 * HttpClient BodyPublisher that streams an InputStream with a known length.
 */
final class InputStreamBodyPublisher implements HttpRequest.BodyPublisher {

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private final InputStream inputStream;
    private final long contentLength;
    private final Executor executor;

    InputStreamBodyPublisher(InputStream inputStream, long contentLength) {
        this(inputStream, contentLength, Infrastructure.getDefaultExecutor());
    }

    InputStreamBodyPublisher(InputStream inputStream, long contentLength, Executor executor) {
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream");
        this.contentLength = contentLength;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public long contentLength() {
        return contentLength;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        InputStreamSubscription subscription = new InputStreamSubscription(subscriber, inputStream, executor);
        subscriber.onSubscribe(subscription);
    }

    private static final class InputStreamSubscription implements Flow.Subscription {
        private final Flow.Subscriber<? super ByteBuffer> subscriber;
        private final InputStream inputStream;
        private final Executor executor;
        private final AtomicLong demand = new AtomicLong(0);
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final Object lock = new Object();
        private volatile boolean cancelled = false;

        InputStreamSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
                                InputStream inputStream,
                                Executor executor) {
            this.subscriber = subscriber;
            this.inputStream = inputStream;
            this.executor = executor;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException("Request must be > 0"));
                cancel();
                return;
            }
            if (cancelled) {
                return;
            }
            demand.getAndAccumulate(n, InputStreamSubscription::addWithCap);
            if (started.compareAndSet(false, true)) {
                executor.execute(this::pump);
            } else {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            closeQuietly();
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        private void pump() {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            try {
                while (!cancelled) {
                    if (demand.get() <= 0) {
                        synchronized (lock) {
                            while (!cancelled && demand.get() <= 0) {
                                lock.wait();
                            }
                        }
                    }
                    if (cancelled) {
                        return;
                    }
                    int read = inputStream.read(buffer);
                    if (read < 0) {
                        subscriber.onComplete();
                        return;
                    }
                    ByteBuffer payload = ByteBuffer.wrap(copyOf(buffer, read));
                    demand.decrementAndGet();
                    subscriber.onNext(payload);
                }
            } catch (IOException e) {
                subscriber.onError(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                subscriber.onError(e);
            } finally {
                closeQuietly();
            }
        }

        private static long addWithCap(long current, long n) {
            long sum = current + n;
            return sum < 0 ? Long.MAX_VALUE : sum;
        }

        private static byte[] copyOf(byte[] buffer, int length) {
            byte[] copy = new byte[length];
            System.arraycopy(buffer, 0, copy, 0, length);
            return copy;
        }

        private void closeQuietly() {
            try {
                inputStream.close();
            } catch (IOException ignored) {
                // best-effort close
            }
        }
    }
}
