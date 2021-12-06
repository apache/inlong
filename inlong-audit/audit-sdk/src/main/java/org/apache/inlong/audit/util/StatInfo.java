package org.apache.inlong.audit.util;

import java.util.concurrent.atomic.AtomicLong;

public class StatInfo {
    public AtomicLong count = new AtomicLong(0);
    public AtomicLong size = new AtomicLong(0);
    public AtomicLong delay = new AtomicLong(0);

    public StatInfo(long cnt, long sz, long dy) {
        count.set(cnt);
        size.set(sz);
        delay.set(dy);
    }
}
