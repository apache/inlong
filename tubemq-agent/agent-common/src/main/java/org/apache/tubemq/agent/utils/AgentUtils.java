/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.utils;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentUtils.class);
    private static final AtomicLong index = new AtomicLong(0);

    /**
     * finally close resources
     *
     * @param resource -  resource which is closable.
     */
    public static void finallyClose(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ex) {
                LOGGER.info("error while closing", ex);
            }
        }
    }

    /**
     * finally close resources.
     *
     * @param resource -  resource which is closable.
     */
    public static void finallyClose(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ex) {
                LOGGER.error("error while closing", ex);
            }
        }
    }

    /**
     * Get declare fields.
     */
    public static List<Field> getDeclaredFieldsIncludingInherited(Class<?> clazz) {
        List<Field> fields = new ArrayList<Field>();
        // 向上查找父类是否有注解
        while (clazz != null) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    /**
     * Get declare methods.
     *
     * @param clazz - class of field from method return
     * @return list of methods
     */
    public static List<Method> getDeclaredMethodsIncludingInherited(Class<?> clazz) {
        List<Method> methods = new ArrayList<Method>();
        while (clazz != null) {
            methods.addAll(Arrays.asList(clazz.getDeclaredMethods()));
            clazz = clazz.getSuperclass();
        }
        return methods;
    }

    /**
     * get random int of [seed, seed * 2]
     * @param seed
     * @return
     */
    public static int getRandomBySeed(int seed) {
        return ThreadLocalRandom.current().nextInt(0, seed) + seed;
    }

    public static String getLocalIP() {
        String ip = "127.0.0.1";
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        } catch (Exception ex) {
            LOGGER.error("error while get local ip", ex);
        }
        return ip;
    }

    /**
     * Get uniq id with timestamp.
     *
     * @return uniq id.
     */
    public static String getUniqId(String id) {
        // timestamp in seconds
        long currentTime = System.currentTimeMillis() / 1000;
        return  "job_" + id + "_" + currentTime + "_" + index.getAndIncrement();
    }

    public static void silenceSleepInMs(long millisecond) {
        try {
            TimeUnit.MILLISECONDS.sleep(millisecond);
        } catch (Exception ignored) {

        }
    }
}
