package org.apache.inlong.audit.util;

import org.junit.jupiter.api.Test;

class ConfigTest {

    @Test
    void getLocalIP() {
        Config test=new Config();
        test.init();
        String ip = test.getLocalIP();
        System.out.println(ip);
    }

    @Test
    void getDockerId() {
        Config test=new Config();
        test.init();
        String dockerId=test.getDockerId();
        System.out.println(dockerId);
    }
}