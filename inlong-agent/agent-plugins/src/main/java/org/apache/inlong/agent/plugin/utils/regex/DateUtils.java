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

package org.apache.inlong.agent.plugin.utils.regex;

import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.utils.DateTransUtils;

import hirondelle.date4j.DateTime;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtils {

    private static final String YEAR = "YYYY";
    private static final String YEAR_LOWERCASE = "yyyy";
    private static final String MONTH = "MM";
    private static final String DAY = "DD";
    private static final String DAY_LOWERCASE = "dd";
    private static final String HOUR = "HH";
    private static final String HOUR_LOWERCASE = "hh";
    private static final String MINUTE = "mm";
    public static final String DEFAULT_FORMAT = "yyyyMMddHHmm";
    public static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";
    private static final Logger logger = LoggerFactory.getLogger(DateUtils.class);
    private static final String TIME_REGEX = "YYYY(?:.MM|MM)?(?:.DD|DD)?(?:.hh|hh)?(?:.mm|mm)?(?:"
            + ".ss|ss)?";
    private static final Pattern pattern = Pattern.compile(TIME_REGEX,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private static final Pattern bracePatt = Pattern.compile("\\{(.*?)\\}");
    private static final int DEFAULT_LENGTH = "yyyyMMddHHmm".length();
    public static long DAY_TIMEOUT_INTERVAL = 2 * 24 * 3600 * 1000;
    public static long HOUR_TIMEOUT_INTERVAL = 2 * 3600 * 1000;

    public static String getShouldStartTime(String dataTime, String cycleUnit,
            String offset) {
        if (dataTime == null || dataTime.length() > 12) {
            return null;
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
        TimeZone timeZone = TimeZone.getTimeZone(DateUtils.DEFAULT_TIME_ZONE);
        dateFormat.setTimeZone(timeZone);

        if (dataTime.length() < DEFAULT_LENGTH) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < DEFAULT_LENGTH - dataTime.length(); i++) {
                sb.append("0");
            }
            dataTime = dataTime + sb.toString();
        }

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTimeInMillis(dateFormat.parse(dataTime).getTime());
        } catch (ParseException e) {
            return null;
        }

        /*
         * The delay should be added to the data time, so remove the - from offset.
         */
        if (offset.startsWith("-")) {
            offset = offset.substring(1, offset.length());
        } else { // positive，read file earlier
            offset = "-" + offset;
        }

        return dateFormat
                .format(new Date(getDateTime(calendar, cycleUnit, offset).getTimeInMillis()));
    }

    private static Calendar getDateTime(Calendar calendar, String cycleUnit, String offset) {
        int cycleNumber = (cycleUnit.length() <= 1 ? 1
                : Integer.parseInt(cycleUnit.substring(0, cycleUnit.length() - 1)));

        String offsetUnit = offset.substring(offset.length() - 1, offset.length());
        int offsetNumber = Integer.parseInt(offset.substring(0, offset.length() - 1));

        /*
         * For day task, the offset cycle unit can only be day; for hourly task, the offset can't be minute; for
         * minutely task, the offset cycle unit can be day, hour and minute, but if the offset cycle unit is minute, the
         * offset must be divided by cycle number.
         */
        if (cycleUnit.length() > 1 && (StringUtils.endsWithIgnoreCase(cycleUnit, "M"))) {
            calendar.set(Calendar.SECOND, 0);
            int minTime = calendar.get(Calendar.MINUTE);

            int leftMin = minTime % cycleNumber;
            minTime = minTime - leftMin;
            calendar.set(Calendar.MINUTE, minTime);

            /* Calculate the offset. */
            if (CycleUnitType.DAY.equalsIgnoreCase(offsetUnit)) {
                calendar.add(Calendar.DAY_OF_YEAR, offsetNumber);
            }

            if (CycleUnitType.HOUR.equalsIgnoreCase(offsetUnit)) {
                calendar.add(Calendar.HOUR_OF_DAY, offsetNumber);
            }
        } else if (cycleUnit.length() == 1) {
            if (CycleUnitType.DAY.equalsIgnoreCase(cycleUnit)) {
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
            } else if (CycleUnitType.HOUR.equalsIgnoreCase(cycleUnit)) {
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
            }
        }

        /* Calculate the offset. */
        if (CycleUnitType.DAY.equalsIgnoreCase(offsetUnit)) {
            calendar.add(Calendar.DAY_OF_YEAR, offsetNumber);
        }

        if (CycleUnitType.HOUR.equalsIgnoreCase(offsetUnit)) {
            calendar.add(Calendar.HOUR_OF_DAY, offsetNumber);
        }

        if (CycleUnitType.MINUTE.equals(offsetUnit)) {
            calendar.add(Calendar.MINUTE, offsetNumber);
        }

        return calendar;
    }

    public static boolean isValidCreationTime(String dataTime, String cycleUnit,
            String timeOffset) {
        long timeInterval = 0;
        if (CycleUnitType.DAY.equalsIgnoreCase(cycleUnit)) {
            timeInterval = DAY_TIMEOUT_INTERVAL;
        } else if (CycleUnitType.HOUR.equalsIgnoreCase(cycleUnit)) {
            timeInterval = HOUR_TIMEOUT_INTERVAL;
        } else if (cycleUnit.endsWith(CycleUnitType.MINUTE)) {
            timeInterval = HOUR_TIMEOUT_INTERVAL;
        } else {
            logger.error("cycleUnit {} can't parse!", cycleUnit);
            timeInterval = DAY_TIMEOUT_INTERVAL;
        }

        if (timeOffset.startsWith("-")) {
            timeInterval -= DateTransUtils.calcOffset(timeOffset);
        } else {
            timeInterval += DateTransUtils.calcOffset(timeOffset);
        }

        return isValidCreationTime(dataTime, timeInterval);
    }

    /*
     * Check whether the data time is between curTime - interval and curTime + interval.
     */
    public static boolean isValidCreationTime(String dataTime, long timeInterval) {
        long currentTime = System.currentTimeMillis();

        long minTime = currentTime - timeInterval;
        long maxTime = currentTime + timeInterval;

        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
        if (dataTime.length() < DEFAULT_LENGTH) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < DEFAULT_LENGTH - dataTime.length(); i++) {
                sb.append("0");
            }
            dataTime = dataTime + sb.toString();
        }

        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTimeInMillis(dateFormat.parse(dataTime).getTime());
        } catch (ParseException e) {
            return false;
        }

        return calendar.getTimeInMillis() >= minTime
                && calendar.getTimeInMillis() <= maxTime;
    }

    public static String getDateTime(String fileName, PathDateExpression dateExpression) {
        if (fileName == null || dateExpression == null
                || dateExpression.getLongestDatePattern() == null) {
            return null;
        }

        String longestDatePattern = DateUtils
                .replaceDateExpressionWithRegex(dateExpression.getLongestDatePattern());
        NonRegexPatternPosition patternPosition = dateExpression.getPatternPosition();

        Matcher mat = Pattern.compile(longestDatePattern).matcher(fileName);
        boolean find = mat.find();
        // TODO : more than one part match the time regex in file name ("/data/joox_logs/2000701106/201602170040.log"
        // YYYYMMDDhh)
        if (!find) {
            logger.error("Can't find the pattern {} for file name {}", longestDatePattern,
                    fileName);
            return null;
        }

        String dateTime = fileName.substring(mat.start(), mat.end());
        if (patternPosition == NonRegexPatternPosition.PREFIX) {
            dateTime = dateTime.substring(1, dateTime.length());
        } else if (patternPosition == NonRegexPatternPosition.SUFFIX) {
            dateTime = dateTime.substring(0, dateTime.length() - 1);
        } else if (patternPosition == NonRegexPatternPosition.BOTH) {
            dateTime = dateTime.substring(1, dateTime.length() - 1);
        } else if (patternPosition == NonRegexPatternPosition.END) {
            dateTime = dateTime.substring(0, dateTime.length());
        } else if (patternPosition == NonRegexPatternPosition.ENDSUFFIX) {
            dateTime = dateTime.substring(1, dateTime.length());
        } else if (patternPosition == NonRegexPatternPosition.NONE) {
            logger.error("The data path configuration is invalid");
            dateTime = null;
        }

        return dateTime;
    }

    public static ArrayList<MatchPoint> extractAllTimeRegex(String src) {
        // TODO : time regex error
        Matcher m = pattern.matcher(src);
        ArrayList<MatchPoint> arr = new ArrayList<MatchPoint>();
        while (m.find()) {
            String oneMatch = m.group(0);
            arr.add(new MatchPoint(oneMatch, m.start(), m.end()));
        }
        return arr;
    }

    public static String replaceDateExpressionWithRegex(String dataPath) {
        if (dataPath == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        // find longest DATEPATTERN
        ArrayList<MatchPoint> mp = extractAllTimeRegex(dataPath);

        if (mp == null || mp.size() == 0) {
            return dataPath;
        }

        int lastIndex = 0;
        for (MatchPoint m : mp) {
            lastIndex = replacePattern(dataPath, sb, m, "\\d{4}", "\\d{2}", "\\d{2}", "\\d{2}", "\\d{2}", lastIndex);
        }

        sb.append(dataPath.substring(lastIndex));

        return sb.toString();
    }

    private static int replacePattern(String dataPath, StringBuffer sb, MatchPoint m, String year, String month,
            String day, String hour, String minute, int lastIndex) {
        sb.append(dataPath, lastIndex, m.getStart());

        String longestPattern = m.getStr();
        int hhIndex = longestPattern.indexOf(HOUR);
        if (hhIndex == -1) {
            hhIndex = longestPattern.indexOf(HOUR_LOWERCASE);
        }
        int mmIndex = longestPattern.indexOf(MINUTE);
        longestPattern = longestPattern.replace(YEAR, year);
        longestPattern = longestPattern.replace(YEAR_LOWERCASE, year);
        longestPattern = longestPattern.replace(MONTH, month);
        longestPattern = longestPattern.replace(DAY, day);
        longestPattern = longestPattern.replace(DAY_LOWERCASE, day);
        longestPattern = longestPattern.replace(HOUR, hour);
        longestPattern = longestPattern.replace(HOUR_LOWERCASE, hour);

        if (hhIndex != -1 && mmIndex != -1
                && mmIndex >= hhIndex + 2 && mmIndex < hhIndex + 4) {
            longestPattern = longestPattern.replace(MINUTE, minute);
        }

        sb.append(longestPattern);
        return m.getEnd();
    }

    public static String replaceDateExpression(Calendar dateTime, String dataPath) {
        if (dataPath == null) {
            return null;
        }

        String year = String.valueOf(dateTime.get(Calendar.YEAR));
        String month = String.valueOf(dateTime.get(Calendar.MONTH) + 1);
        String day = String.valueOf(dateTime.get(Calendar.DAY_OF_MONTH));
        String hour = String.valueOf(dateTime.get(Calendar.HOUR_OF_DAY));
        String minute = String.valueOf(dateTime.get(Calendar.MINUTE));

        StringBuffer sb = new StringBuffer();

        // find longest DATEPATTERN
        ArrayList<MatchPoint> mp = extractAllTimeRegex(dataPath);
        if (mp == null || mp.size() == 0) {
            return dataPath;
        }

        int lastIndex = 0;
        for (MatchPoint m : mp) {
            lastIndex = replacePattern(dataPath, sb, m, year, externDate(month), externDate(day), externDate(hour),
                    externDate(minute), lastIndex);
        }

        sb.append(dataPath.substring(lastIndex));

        return sb.toString();
    }

    public static String externDate(String time) {
        if (time.length() == 1) {
            return "0" + time;
        } else {
            return time;
        }
    }

    public static List<Long> getDateRegion(long startTime, long endTime, String cycleUnit) {
        List<Long> ret = new ArrayList<Long>();
        DateTime dtStart = DateTime.forInstant(startTime, TimeZone.getDefault());
        DateTime dtEnd = DateTime.forInstant(endTime, TimeZone.getDefault());

        if (cycleUnit.equals(CycleUnitType.DAY)) {
            dtEnd = dtEnd.getEndOfDay();
        }

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        if (cycleUnit.equalsIgnoreCase(CycleUnitType.DAY)) {
            day = 1;
        } else if (cycleUnit.equalsIgnoreCase(CycleUnitType.HOUR)) {
            hour = 1;
        } else if (cycleUnit.equals(CycleUnitType.MINUTE)) {
            minute = 1;
        } else {
            logger.error("cycleUnit {} is error: ", cycleUnit);
            return ret;
        }
        while (dtStart.lteq(dtEnd)) {
            ret.add(dtStart.getMilliseconds(TimeZone.getDefault()));
            dtStart = dtStart.plus(year, month, day, hour, minute, second, 0,
                    DateTime.DayOverflow.LastDay);
        }
        return ret;
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
}
