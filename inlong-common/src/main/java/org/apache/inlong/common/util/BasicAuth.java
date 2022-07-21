package org.apache.inlong.common.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class BasicAuth {

    /**
     * Generate http basic auth credential from configured secretId and secretKey
     */
    public static String genBasicAuthCredential(String secretId, String secretKey) {
        String credential = String.join(":", secretId, secretKey);
        return "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
    }
}
