package org.apache.inlong.manager.service.resource.sink;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.misc.BASE64Encoder;

public class Base64CompatibleTest {

    @Test
    public void base64CompatibleForDifferentVersionTest() {
        String tokenStr = "root" + ":" + "admin";
        byte[] tokeBytes = tokenStr.getBytes(StandardCharsets.UTF_8);
        // token encode by sun.misc.BASE64Encoder, this test will fail if the jdk version is 9 or later
        // so, this test should be removed when Apache InLong project support jdk 9 or later
        String tokenOld = String.valueOf(new BASE64Encoder().encode(tokeBytes));
        // token encode by java.util.Base64.Encoder
        Encoder encoder = Base64.getEncoder();
        String tokenNew = encoder.encodeToString(tokeBytes);
        Assertions.assertEquals(tokenOld, tokenNew);
    }
}
