package org.apache.inlong.dataproxy.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: baiyiyang
 * Date: 15-9-29
 */
public class LogCounter {

    private AtomicInteger counter = new AtomicInteger(0);

    private int start = 10;
    private int control = 1000;
    private int reset = 60 * 1000;

    private long lastLogTime = System.currentTimeMillis();

    public LogCounter(int start, int control, int reset) {
        this.start = start;
        this.control = control;
        this.reset = reset;
    }

    public boolean shouldPrint() {
        if (System.currentTimeMillis() - lastLogTime > reset) {
            counter.set(0);
            this.lastLogTime = System.currentTimeMillis();
        }

        if (counter.incrementAndGet() > start && counter.get() % control != 0) {
            return false;
        }

        return true;
    }
}
