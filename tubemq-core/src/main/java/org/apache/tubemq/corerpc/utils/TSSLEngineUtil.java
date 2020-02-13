/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corerpc.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

public class TSSLEngineUtil {

    public static SSLEngine createSSLEngine(String keyStorePath, String trustStorePath,
                                            String keyStorePassword, String trustStorePassword,
                                            boolean isClientMode, boolean needTwyWayAuth)
            throws IOException, CertificateException,
            NoSuchAlgorithmException, UnrecoverableKeyException,
            KeyStoreException, KeyManagementException {
        KeyManagerFactory kmf = null;
        TrustManagerFactory tmf = null;
        if (isClientMode || needTwyWayAuth) {
            FileInputStream fileInputStream = null;
            KeyStore ts = KeyStore.getInstance("JKS");
            try {
                fileInputStream = new FileInputStream(new File(trustStorePath));
                ts.load(fileInputStream, trustStorePassword.toCharArray());
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }

        }
        if (!isClientMode || needTwyWayAuth) {
            FileInputStream fileInputStream = null;
            KeyStore ks = KeyStore.getInstance("JKS");
            try {
                fileInputStream = new FileInputStream(new File(keyStorePath));
                ks.load(fileInputStream, keyStorePassword.toCharArray());
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyStorePassword.toCharArray());
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
        }
        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf == null ? null : kmf.getKeyManagers(),
                tmf == null ? null : tmf.getTrustManagers(), null);
        SSLEngine sslEngine = serverContext.createSSLEngine();
        sslEngine.setUseClientMode(isClientMode);
        sslEngine.setNeedClientAuth(needTwyWayAuth);
        return sslEngine;
    }
}
