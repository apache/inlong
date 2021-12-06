package org.apache.inlong.audit.util;

import org.junit.jupiter.api.Test;

class IpPortTest {
    private IpPort test = new IpPort("127.0.0.1", 80);
    @Test
    void getIpPortKey() {
        String ipPortKey=test.getIpPortKey("127.0.0.1",80);
        System.out.println(ipPortKey);
    }

    @Test
    void testHashCode() {
        int hashCode=test.hashCode();
        System.out.println(hashCode);
    }

    @Test
    void testEquals() {
        IpPort test1=new IpPort("127.0.0.1",81);
        boolean ret=test.equals(test1);
        System.out.println(ret);

        IpPort test2=new IpPort("127.0.0.1",80);
        ret=test.equals(test2);
        System.out.println(ret);

        IpPort test3=test;
        ret=test.equals(test3);
        System.out.println(ret);
    }

    @Test
    void parseIpPort() {
        IpPort testIpPort=test.parseIpPort("127.0.0.1:83");
        System.out.println(testIpPort);
    }

    @Test
    void testToString() {
        String toSteing=test.toString();
        System.out.println(toSteing);
    }
}