package org.apache.inlong.sdk.dataproxy.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class ConsistencyHashUtil {
    public static String hashMurMurHash(String key, int seed) {
        HashFunction hashFunction = Hashing.murmur3_128(seed);
        return hashFunction.hashString(key, StandardCharsets.UTF_8).toString();
    }

    public static String hashMurMurHash(String key) {
        HashFunction hashFunction = Hashing.murmur3_128();
        return hashFunction.hashString(key, StandardCharsets.UTF_8).toString();
    }
}
