package org.apache.inlong.audit.util;


import org.junit.Test;

public class ConfigTest {

    @Test
    public void getLocalIP() {
        Config test = new Config();
        test.init();
        String ip = test.getLocalIP();
        System.out.println(ip);
    }

    @Test
    public void getDockerId() {
        Config test = new Config();
        test.init();
        String dockerId = test.getDockerId();
        System.out.println(dockerId);
    }
}