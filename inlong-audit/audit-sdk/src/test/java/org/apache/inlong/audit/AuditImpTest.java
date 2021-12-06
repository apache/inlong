package org.apache.inlong.audit;

import org.junit.jupiter.api.Test;

class AuditImpTest {

    @Test
    void getInstance() {
    }

    @Test
    void init() {
        AuditImp.getInstance().init("inlog_audit.txt");
    }

    @Test
    void add() {
        AuditImp.getInstance().init("inlog_audit.txt");
        AuditImp.getInstance().add(1, "inlongGroupIDTest",
                "inlongStreamIDTest", System.currentTimeMillis(), 1, 1);
    }
}