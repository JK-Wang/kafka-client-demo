package io.kkk.kafka.demo;

import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.*;

public class ThreadPool {
    public static final ThreadPoolExecutor KAFKA_ADMIN_CLIENT_THREAD_POOL = new ThreadPoolExecutor(
            8,
            16,
            120L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
    );

    public static Future<KafkaFuture<Void>> submitKafkaAdminClientTask(Callable<KafkaFuture<Void>> callable) {
        return KAFKA_ADMIN_CLIENT_THREAD_POOL.submit(callable);
    }

    public static void shutdown() {
        KAFKA_ADMIN_CLIENT_THREAD_POOL.shutdown();
    }
}
