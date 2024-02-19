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

package org.apache.inlong.agent.plugin.utils.file;

import hirondelle.date4j.DateTime;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtils {

    private static final Logger logger = LogManager.getLogger(DateUtils.class);
    private static final String TIME_REGEX = "YYYY(?:.MM|MM)?(?:.DD|DD)?(?:.hh|hh)?(?:.mm|mm)?(?:"
            + ".ss|ss)?";
    private static final String LIMIT_SEP = "(?<=[a-zA-Z])";
    private static final String LETTER_STR = "\\D+";
    private static final String DIGIT_STR = "[0-9]+";
    private static final Pattern pattern = Pattern.compile(TIME_REGEX,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private String dateFormat = "YYYYMMDDhhmmss";

    public DateUtils() {

    }

    public DateUtils(String timeFormat) {
        if (timeFormat != null && !timeFormat.isEmpty()) {
            dateFormat = timeFormat;
        }
    }

    public static String getSubTimeFormat(String format, int length) {
        // format may be "YYYYMMDDhhmmss" | "YYYY_MM_DD_hh_mm_ss"
        int formatLen = format.length();
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < formatLen && length > 0; ++i) {
            if (Character.isLetter(format.charAt(i))
                    || Character.isDigit(format.charAt(i))) {
                length--;
            }
            sb.append(format.charAt(i));
        }
        return sb.toString();
    }

    // only return the first matched
    public static String extractLongestTimeRegex(String src)
            throws IllegalArgumentException {
        Matcher m = pattern.matcher(src);
        String ret = "";
        while (m.find()) {
            String oneMatch = m.group(0);
            if (oneMatch.length() > ret.length()) {
                ret = oneMatch;
            }
        }
        return ret;
    }

    public static PathDateExpression extractLongestTimeRegexWithPrefixOrSuffix(String src)
            throws IllegalArgumentException {
        if (src == null) {
            return null;
        }

        String longestPattern = extractLongestTimeRegex(src);
        if (longestPattern.isEmpty()) {
            return new PathDateExpression(longestPattern, NonRegexPatternPosition.NONE);
        }
        String regexSign = "\\^$*+?{(|[)]";

        String range = "+?*{";

        int beginIndex = src.indexOf(longestPattern);
        int endIndex = beginIndex + longestPattern.length();
        String prefix = src.substring(beginIndex - 1, beginIndex);

        NonRegexPatternPosition position = NonRegexPatternPosition.NONE;
        if (!regexSign.contains(prefix)) {
            longestPattern = prefix + longestPattern;
            position = NonRegexPatternPosition.PREFIX;
        }
        String suffix = "";
        if (src.length() > endIndex) {
            suffix = src.substring(endIndex, endIndex + 1);
        }
        boolean bFlag = false;

        if (Objects.equals(suffix, ".") && src.length() > endIndex + 1) {

            char c = src.charAt(endIndex + 1);
            if (StringUtils.indexOf(range, c) != -1) {
                bFlag = true;
            }
        }

        if (!Objects.equals(suffix, "") && !regexSign.contains(suffix) && !bFlag) {
            longestPattern = longestPattern + suffix;
            if (position == NonRegexPatternPosition.PREFIX) {
                position = NonRegexPatternPosition.BOTH;
            } else {
                position = NonRegexPatternPosition.SUFFIX;
            }
        }
        if (Objects.equals(suffix, "")) {
            if (position == NonRegexPatternPosition.PREFIX) {
                position = NonRegexPatternPosition.ENDSUFFIX;
            } else {
                position = NonRegexPatternPosition.END;
            }
        }

        return ((position == NonRegexPatternPosition.NONE) ? null
                : new PathDateExpression(longestPattern, position));
    }

    public static String formatTime(long time) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
        df.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
        return df.format(new Date(time));
    }

    public static boolean compare(String time, int offset)
            throws ParseException {
        long value = 1000 * 60 * 60 * 24;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        long to = System.currentTimeMillis();
        long from = df.parse(time.substring(0, 8)).getTime();
        if ((to - from) / value > offset) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean compare(long time, int offset) {
        long value = 1000 * 60 * 60 * 24;
        long to = System.currentTimeMillis();
        if ((to - time) / value > offset) {
            return true;
        } else {
            return false;
        }
    }

    public void init(String timeFormat) {
        if (timeFormat != null && !timeFormat.isEmpty()) {
            dateFormat = timeFormat;
        }
    }

    // 20120812010203 ---> 2012-08-12 01:02:03
    private String normalizeDateStr(String src) {
        src = src.replaceAll("[^a-zA-Z0-9]", "");
        int len = src.length();
        // if (!isTimeStrValid(src)) {
        // return "";
        // }
        StringBuffer sb = new StringBuffer();
        // year
        sb.append(src.substring(0, 4));
        sb.append("-");
        if (len > 4) {
            // month
            sb.append(src.substring(4, 6));
            if (len > 6) {
                sb.append("-");
                // day
                sb.append(src.substring(6, 8));
                if (len > 8) {
                    sb.append(" ");
                    // hour
                    sb.append(src.substring(8, 10));
                    if (len > 10) {
                        sb.append(":");
                        // minute
                        sb.append(src.substring(10, 12));
                        if (len > 12) {
                            sb.append(":");
                            // seconds
                            sb.append(src.substring(12, 14));
                        } else {
                            sb.append(":00");
                        }
                    } else {
                        sb.append(":00:00");
                    }
                } else {
                    sb.append(" 00:00:00");
                }
            } else {
                sb.append("-01 00:00:00");
            }
        } else {
            sb.append("-01-01 00:00:00");
        }
        return sb.toString();
    }

    public String getFormatSpecifiedTime(String specifiedTime) {
        if (specifiedTime == null || specifiedTime.length() == 0) {
            return specifiedTime;
        }

        int formatLen = dateFormat.length();

        if (specifiedTime.length() == formatLen
                && !specifiedTime.matches(DIGIT_STR)) {
            return specifiedTime;
        }

        StringBuilder retSb = new StringBuilder();
        int specifiedInx = 0;
        for (int i = 0; i < formatLen; i++) {
            char tmpChar = dateFormat.charAt(i);

            if (tmpChar != 'Y' && tmpChar != 'M' && tmpChar != 'D'
                    && tmpChar != 'h' && tmpChar != 'm') {
                retSb.append(tmpChar);
            } else {
                retSb.append(specifiedTime.charAt(specifiedInx++));
            }
        }

        logger.info(
                "TimeRegex {} <> specifiedTime {} not match, format specifiedTime {}",
                new Object[]{dateFormat, specifiedTime, retSb.toString()});

        return retSb.toString();
    }

    public String getDate(String src, String limit) {
        if (src == null || src.trim().isEmpty()) {
            return "";
        }

        // TODO : verify format str
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;

        // TODO : timezone
        TimeZone tz = TimeZone.getTimeZone("GMT+8:00");
        DateTime dt = null;
        String outputFormat = null;
        if (src.matches(LETTER_STR)) {
            // format str
            // TODO : data format verify
            dt = DateTime.now(tz);
            outputFormat = src;
        } else {
            // time str
            src = src.replaceAll("[^0-9]", "");
            outputFormat = getSubTimeFormat(dateFormat, src.length());
            src = normalizeDateStr(src);
            if (src.isEmpty()) {
                return "";
            }
            dt = new DateTime(src);
        }

        // System.out.println("outputformat: " + outputFormat);

        limit = limit.trim();
        String[] limitArr = limit.split(LIMIT_SEP);

        for (String onelimit : limitArr) {
            year = 0;
            month = 0;
            day = 0;
            hour = 0;
            minute = 0;
            second = 0;
            // System.out.println("onelimit: " + onelimit);
            int limitLen = onelimit.length();
            String type = onelimit.substring(limitLen - 1, limitLen);
            int offset = Integer.parseInt(onelimit.substring(0, limitLen - 1));
            // System.out.println("type: " + type + ". offset: " + offset);
            int sign = 1;
            if (offset < 0) {
                sign = -1;
            } else {
                sign = 1;
            }
            if (type.equalsIgnoreCase("Y")) {
                year = sign * offset;
            } else if (type.equals("M")) {
                month = sign * offset;
            } else if (type.equalsIgnoreCase("D")) {
                day = sign * offset;
            } else if (type.equalsIgnoreCase("h")) {
                hour = sign * offset;
            } else if (type.equals("m")) {
                minute = sign * offset;
            } else if (type.equalsIgnoreCase("s")) {
                second = sign * offset;
            }
            if (sign < 0) {
                dt = dt.minus(year, month, day, hour, minute, second, 0,
                        DateTime.DayOverflow.LastDay);
            } else {
                dt = dt.plus(year, month, day, hour, minute, second, 0,
                        DateTime.DayOverflow.LastDay);
            }

        }
        return dt.format(outputFormat);
    }

    public String getAttrPunit(String attrs) {
        String punit = null;
        if (attrs != null && attrs.contains("&p=")) {
            for (String attr : attrs.split("&")) {
                if (attr.startsWith("p=") && attr.split("=").length == 2) {
                    punit = attr.split("=")[1];
                    break;
                }
            }
        }

        return punit;
    }

    public String getSpecifiedDate(String src, String limit, String punit) {
        String ret = getDate(src, limit);
        return formatCurrPeriod(ret, punit);
    }

    public String normalizeTimeRegex(String src) {
        return getSubTimeFormat(dateFormat, src.length());
    }

    public String getCurrentDir(String src, String timeOffset) {
        Matcher m = pattern.matcher(src);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String oneMatch = m.group(0);
            String currTimeStr = getDate(oneMatch, timeOffset);
            m.appendReplacement(sb, currTimeStr);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public String getCurrentDirByPunit(String src, String timeOffset,
            String punit) {
        Matcher m = pattern.matcher(src);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String oneMatch = m.group(0);
            String currTimeStr = getSpecifiedDate(oneMatch, timeOffset, punit);
            m.appendReplacement(sb, currTimeStr);
        }
        m.appendTail(sb);

        return sb.toString();
    }

    public String getSpecifiedDir(String src, String specifiedDate) {
        Matcher m = pattern.matcher(src);
        StringBuffer sb = new StringBuffer();

        while (m.find()) {
            String oneMatch = m.group(0);
            StringBuilder tmpSb = new StringBuilder();
            int specifiedDateIdx = 0;

            for (int i = 0; i < oneMatch.length(); i++) {
                char matchChar = oneMatch.charAt(i);
                if (matchChar != 'Y' && matchChar != 'M' && matchChar != 'D'
                        && matchChar != 'h' && matchChar != 'm') {
                    tmpSb.append(matchChar);
                } else {
                    char dateChar = specifiedDate.charAt(specifiedDateIdx);
                    while (String.valueOf(dateChar).matches("\\D")) {
                        dateChar = specifiedDate.charAt(++specifiedDateIdx);
                    }
                    tmpSb.append(dateChar);
                    specifiedDateIdx++;
                }
            }
            m.appendReplacement(sb, tmpSb.toString());
        }
        m.appendTail(sb);
        return sb.toString();
    }

    // format current period starting less-than-hour task
    // * for example: ten-minute task:
    // * currPeriodDataTime is 201303271905
    // * formated value is 201303271900
    public String formatCurrPeriod(String src, String punit) {
        if (src == null || punit == null || src.length() != 12) {
            return src;
        }

        String prefixMinuteStr = src.substring(0, src.length() - 2);
        String minuteStr = src.substring(src.length() - 2, src.length());

        if ("n".equals(punit)) {
            if (minuteStr.compareTo("30") < 0) {
                minuteStr = "00";
            } else {
                minuteStr = "30";
            }
        } else if ("q".equals(punit)) {
            if (minuteStr.compareTo("15") < 0) {
                minuteStr = "00";
            } else if (minuteStr.compareTo("30") < 0) {
                minuteStr = "15";
            } else if (minuteStr.compareTo("45") < 0) {
                minuteStr = "30";
            } else {
                minuteStr = "45";
            }
        } else if ("t".equals(punit)) {
            minuteStr = minuteStr.charAt(0) + "0";
        } else if ("f".equals(punit)) {
            if (minuteStr.substring(1).compareTo("5") < 0) {
                minuteStr = minuteStr.charAt(0) + "0";
            } else {
                minuteStr = minuteStr.charAt(0) + "5";
            }
        }

        return prefixMinuteStr + minuteStr;
    }
}
