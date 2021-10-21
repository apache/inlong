package org.apache.inlong.dataproxy.base;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @Auther: nicobao
 * @Date: 2021/10/18 16:29
 * @Description:
 */
public class HighPriorityThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final AtomicInteger threadNumber;
    final ThreadGroup group;
    final String namePrefix;
    final boolean isDaemon;

    public HighPriorityThreadFactory() {
        this("pool");
    }

    public HighPriorityThreadFactory(String name) {
        this(name, false);
    }

    public HighPriorityThreadFactory(String prefix, boolean daemon) {
        this.threadNumber = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = prefix + "-thread-" + poolNumber.getAndIncrement();
        this.isDaemon = daemon;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
        t.setDaemon(this.isDaemon);
        t.setPriority(10);
        return t;
    }
}

