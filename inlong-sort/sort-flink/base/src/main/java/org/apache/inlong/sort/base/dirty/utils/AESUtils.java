/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.base.dirty.utils;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.SecureRandom;

/**
 * AES encryption and decryption utils.
 */
@Slf4j
public class AESUtils {

    private static final int KEY_SIZE = 128;
    private static final String ALGORITHM = "AES";
    private static final String RNG_ALGORITHM = "SHA1PRNG";

    /**
     * Generate key
     */
    private static SecretKey generateKey(byte[] aesKey) throws Exception {
        SecureRandom random = SecureRandom.getInstance(RNG_ALGORITHM);
        random.setSeed(aesKey);
        KeyGenerator gen = KeyGenerator.getInstance(ALGORITHM);
        gen.init(KEY_SIZE, random);
        return gen.generateKey();
    }

    /**
     * Encrypt by key
     */
    public static byte[] encrypt(byte[] plainBytes, byte[] key) throws Exception {
        SecretKey secKey = generateKey(key);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secKey);
        return cipher.doFinal(plainBytes);
    }

    /**
     * Decrypt by key and specified version
     */
    public static byte[] decrypt(byte[] cipherBytes, byte[] key) throws Exception {
        SecretKey secKey = generateKey(key);
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secKey);
        return cipher.doFinal(cipherBytes);
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
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }
}
