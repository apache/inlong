/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.common.pojo.common;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Properties;

public class EncryptUtil {
    private static final Logger logger =
            LoggerFactory.getLogger(EncryptUtil.class);

    private static final int KEY_SIZE = 128;

    private static final String ALGORITHM = "AES";

    private static final String CONFIG_FILE = "application.properties";

    private static final String CONFIG_KEY = "inlong.encrypt.deskey";
    private static final String RNG_ALGORITHM = "SHA1PRNG";

    public static String DES_KEY;

    /**
     * Get deskey from config file
     */
    public static byte[] getDesKeyByConfig() throws Exception {
        Properties properties = new Properties();
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath() + CONFIG_FILE;
        InputStream inputStream = new BufferedInputStream(Files.newInputStream(Paths.get(path)));
        properties.load(inputStream);
        String confDesKey = properties.getProperty(CONFIG_KEY);
        if (StringUtils.isEmpty(confDesKey)) {
            logger.error("cannot find deskey in property");
            throw new Exception("cannot find deskey in property");
        }
        return confDesKey.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Init deskey from property
     */
    private static void initDesKeyByConfig() throws Exception {
        DES_KEY = new String(getDesKeyByConfig(), StandardCharsets.UTF_8);
    }

    /**
     * Generate key
     */
    private static SecretKey generateKey(byte[] desKey) throws Exception {
        SecureRandom random = SecureRandom.getInstance(RNG_ALGORITHM);
        random.setSeed(desKey);
        KeyGenerator gen = KeyGenerator.getInstance(ALGORITHM);
        gen.init(KEY_SIZE, random);
        return gen.generateKey();
    }

    /**
     * Encrypt by key
     */
    public static byte[] encrypt(byte[] plainBytes, byte[] desKey) throws Exception {
        SecretKey key = generateKey(desKey);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, key);

        return cipher.doFinal(plainBytes);
    }

    /**
     * Encrypt by property key
     */
    public static String encryptByConfigToString(byte[] plainBytes) throws Exception {
        if (StringUtils.isEmpty(DES_KEY)) {
            initDesKeyByConfig();
        }
        return parseByte2HexStr(encrypt(plainBytes, DES_KEY.getBytes()));
    }

    /**
     * Decrypt by key
     */
    public static byte[] decrypt(byte[] cipherBytes, byte[] key) throws Exception {
        SecretKey secKey = generateKey(key);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secKey);

        return cipher.doFinal(cipherBytes);
    }

    /**
     * Encrypt by property key
     */
    public static String decryptByConfigAsString(String cipherBytes) throws Exception {
        if (StringUtils.isEmpty(DES_KEY)) {
            initDesKeyByConfig();
        }
        return new String(decrypt(parseHexStr2Byte(cipherBytes), DES_KEY.getBytes()));
    }

    /**
     * Parse byte to String in Hex type
     */
    public static String parseByte2HexStr(byte[] buf) {
        StringBuilder strBuf = new StringBuilder();
        for (byte b : buf) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            strBuf.append(hex.toUpperCase());
        }
        return strBuf.toString();
    }

    /**
     * Parse String to byte as Hex type
     */
    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
            result[i] = (byte)(high * 16 + low);
        }
        return result;
    }
}
