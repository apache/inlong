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

package org.apache.inlong.agent.utils;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentUtils.class);
    private static final AtomicLong INDEX = new AtomicLong(0);
    private static final String HEX_PREFIX = "0x";
    public static final String EQUAL = "=";
    public static final String M_VALUE = "m";
    public static final String ADDITION_SPLITTER = "&";
    public static final String BEIJING_TIME_ZONE = "GMT+8:00";
    public static final String HOUR_PATTERN = "yyyyMMddHH";
    public static final String DAY_PATTERN = "yyyyMMdd";
    public static final String DEFAULT_PATTERN = "yyyyMMddHHmm";
    public static final String DAY = "D";
    public static final String HOUR = "H";
    public static final String HOUR_LOW_CASE = "h";
    public static final String MINUTE = "m";

    /**
     * get md5 of file.
     * @param file - file name
     * @return
     */
    public static String getFileMd5(File file) {
        try (InputStream is = Files.newInputStream(Paths.get(file.getAbsolutePath()))) {
            return DigestUtils.md5Hex(is);
        } catch (Exception ex) {
            LOGGER.warn("cannot get md5 of {}", file, ex);
        }
        return "";
    }

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
        // check whether parent exists
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

    public static String getLocalIp() {
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
    public static String getUniqId(String prefix, String id) {
        return getUniqId(prefix, id, 0L);
    }

    /**
     * Get uniq id with timestamp and index.
     * @param id - job id
     * @param index - job index
     * @return uniq id
     */
    public static String getUniqId(String prefix, String id, long index) {
        long currentTime = System.currentTimeMillis() / 1000;
        return  prefix + currentTime + "_" + id + "_" + index;
    }

    public static void silenceSleepInMs(long millisecond) {
        try {
            TimeUnit.MILLISECONDS.sleep(millisecond);
        } catch (Exception ignored) {
            LOGGER.warn("silenceSleepInMs ", ignored);
        }
    }

    public static String parseHexStr(String delimiter) throws IllegalArgumentException {
        if (delimiter.trim().toLowerCase().startsWith(HEX_PREFIX)) {
            //only one char
            byte[] byteArr = new byte[1];
            byteArr[0] = Byte.decode(delimiter.trim());
            return new String(byteArr, StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("delimiter not start with " + HEX_PREFIX);
        }
    }

    /**
     * formatter for current time
     * @param formatter
     * @return
     */
    public static String formatCurrentTime(String formatter) {
        return formatCurrentTime(formatter, Locale.getDefault());
    }

    public static String formatCurrentTime(String formatter, Locale locale) {
        ZonedDateTime zoned = ZonedDateTime.now();
        // TODO: locale seems not working
        return DateTimeFormatter.ofPattern(formatter).withLocale(locale).format(zoned);
    }

    /**
     * formatter with time offset
     * @param formatter - formatter string
     * @param day - day offset
     * @param hour - hour offset
     * @param min - min offset
     * @return current time with offset
     */
    public static String formatCurrentTimeWithOffset(String formatter, int day, int hour, int min) {
        ZonedDateTime zoned = ZonedDateTime.now().plusDays(day).plusHours(hour).plusMinutes(min);
        return DateTimeFormatter.ofPattern(formatter).withLocale(Locale.getDefault()).format(zoned);
    }

    public static String formatCurrentTimeWithoutOffset(String formatter) {
        ZonedDateTime zoned = ZonedDateTime.now().plusDays(0).plusHours(0).plusMinutes(0);
        return DateTimeFormatter.ofPattern(formatter).withLocale(Locale.getDefault()).format(zoned);
    }

    /**
     * whether all class of path name are matched
     *
     * @param pathStr - path string
     * @param patternStr - regex pattern
     * @return true if all match
     */
    public static boolean regexMatch(String pathStr, String patternStr) {
        String[] pathNames = StringUtils.split(pathStr, FileSystems.getDefault().getSeparator());
        String[] patternNames = StringUtils
                .split(patternStr, FileSystems.getDefault().getSeparator());
        for (int i = 0; i < pathNames.length && i < patternNames.length; i++) {
            if (!pathNames[i].equals(patternNames[i])) {
                Matcher matcher = Pattern.compile(patternNames[i]).matcher(pathNames[i]);
                if (!matcher.matches()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * parse addition attr, the attributes must be send in proxy sender
     * @param additionStr
     * @return
     */
    public static Pair<String, Map<String, String>> parseAddAttr(String additionStr) {
        Map<String, String> attr = new HashMap<>();
        String[] split = additionStr.split(ADDITION_SPLITTER);
        String mValue = "";
        for (String s : split) {
            if (!s.contains(EQUAL)) {
                continue;
            }
            String[] pairs = s.split(EQUAL);
            if (pairs[0].equalsIgnoreCase(M_VALUE)) {
                mValue = pairs[1];
                continue;
            }
            getAttrs(attr, s, pairs);
        }
        return Pair.of(mValue, attr);
    }

    /**
     * the attrs in pairs can be complicated in online env
     * @param attr
     * @param s
     * @param pairs
     */
    private static void getAttrs(Map<String, String> attr, String s, String[] pairs) {
        // when addiction attr be like "m=10&__addcol1__worldid="
        if (s.endsWith(EQUAL) && pairs.length == 1) {
            attr.put(pairs[0], "");
        } else {
            attr.put(pairs[0], pairs[1]);
        }
    }

    /**
     * get addition attributes in additionStr
     * @param additionStr
     * @return
     */
    public static Map<String, String> getAdditionAttr(String additionStr) {
        Pair<String, Map<String, String>> mValueAttrs = parseAddAttr(additionStr);
        return mValueAttrs.getRight();
    }

    /**
     * get m value in additionStr
     * @param addictiveAttr
     * @return
     */
    public static String getmValue(String addictiveAttr) {
        Pair<String, Map<String, String>> mValueAttrs = parseAddAttr(addictiveAttr);
        return mValueAttrs.getLeft();
    }

    /**
     * time str convert to mill sec
     * @param time
     * @param cycleUnit
     * @return
     */
    public static long timeStrConvertToMillSec(String time, String cycleUnit) {
        long defaultTime = System.currentTimeMillis();
        if (time.isEmpty() || cycleUnit.isEmpty()) {
            return defaultTime;
        }
        String pattern = DEFAULT_PATTERN;
        switch (cycleUnit) {
            case DAY:
                pattern = DAY_PATTERN;
                time = time.substring(0, 8);
                break;
            case HOUR:
            case HOUR_LOW_CASE:
                pattern = HOUR_PATTERN;
                time = time.substring(0, 10);
                break;
            case MINUTE:
                break;
            default:
                LOGGER.error("cycle unit {} is illegal, please check!", cycleUnit);
                break;
        }
        return parseTimeToMillSec(time, pattern);
    }

    private static long parseTimeToMillSec(String time, String pattern) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern);
            df.setTimeZone(TimeZone.getTimeZone(BEIJING_TIME_ZONE));
            return df.parse(time).getTime();
        } catch (ParseException e) {
            LOGGER.error("convert time string {} to millSec error", time);
        }
        return System.currentTimeMillis();
    }

}
