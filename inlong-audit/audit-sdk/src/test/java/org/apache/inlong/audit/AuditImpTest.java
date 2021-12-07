package org.apache.inlong.audit;


import org.junit.Test;

public class AuditImpTest {
    @Test
    public void init() {
        AuditImp.getInstance().init("inlog_audit.txt");
    }

    @Test
    public void add() {
        AuditImp.getInstance().init("inlog_audit.txt");
        AuditImp.getInstance().add(1, "inlongGroupIDTest",
                "inlongStreamIDTest", System.currentTimeMillis(), 1, 1);
    }
}