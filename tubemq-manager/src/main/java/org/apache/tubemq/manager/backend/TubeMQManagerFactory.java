package org.apache.tubemq.manager.backend;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread factory for tubeMQ manager.
 */
public class TubeMQManagerFactory implements ThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TubeMQManagerFactory.class);

    private final AtomicInteger mThreadNum = new AtomicInteger(1);

    private final String threadType;

    public TubeMQManagerFactory(String threadType) {
        this.threadType = threadType;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, threadType + "-running-thread-" + mThreadNum.getAndIncrement());
        LOGGER.info("{} created", t.getName());
        return t;
    }
}