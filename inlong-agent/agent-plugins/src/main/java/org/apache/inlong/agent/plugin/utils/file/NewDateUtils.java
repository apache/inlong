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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NewDateUtils {

    public static final String FULL_FORMAT = "yyyyMMddHHmmss";
    public static final String NULL_DATA_TIME = "000000000000";
    public static final String DEFAULT_FORMAT = "yyyyMMddHHmm";
    public static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";
    private static final Logger logger = LoggerFactory.getLogger(NewDateUtils.class);
    private static final String TIME_REGEX = "YYYY(?:.MM|MM)?(?:.DD|DD)?(?:.hh|hh)?(?:.mm|mm)?(?:"
            + ".ss|ss)?";
    private static final String LIMIT_SEP = "(?<=[a-zA-Z])";
    private static final String LETTER_STR = "\\D+";
    private static final String DIGIT_STR = "[0-9]+";
    private static final Pattern pattern = Pattern.compile(TIME_REGEX,
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private static final Pattern bracePatt = Pattern.compile("\\{(.*?)\\}");
    private static final int DEFAULT_LENGTH = "yyyyMMddHHmm".length();
    public static long DAY_TIMEOUT_INTERVAL = 2 * 24 * 3600 * 1000;
    public static long HOUR_TIMEOUT_INTERVAL = 2 * 3600 * 1000;
    // data source config error */
    public static final String DATA_SOURCE_CONFIG_ERROR = "ERROR-0-TDAgent|10001|ERROR"
            + "|ERROR_DATA_SOURCE_CONFIG|";

    /* Return the time in milliseconds for a data time. */
    /*
     * public static long getTimeInMillis(String dataTime) { if (dataTime == null) { return 0; }
     *
     * try { SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_FORMAT); Date date = dateFormat.parse(dataTime);
     *
     * return date.getTime(); } catch (ParseException e) { return 0; } }
     */
    /* Return the format data time string from milliseconds. */
    /*
     * public static String getDataTimeFromTimeMillis(long dataTime) { SimpleDateFormat dateFormat = new
     * SimpleDateFormat(DEFAULT_FORMAT); return dateFormat.format(new Date(dataTime)); }
     */
    /* Return the should start time for a data time log file. */
    public static String getShouldStartTime(String dataTime, String cycleUnit,
            String offset) {
        if (dataTime == null || dataTime.length() > 12) {
            return null;
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
        TimeZone timeZone = TimeZone.getTimeZone(NewDateUtils.DEFAULT_TIME_ZONE);
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

    private static Calendar getCurDate(String cycleUnit, String offset) {
        if (cycleUnit == null || cycleUnit.length() == 0) {
            return null;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());

        return getDateTime(calendar, cycleUnit, offset);
    }

    public static String getDateTime(String dataTime, String cycleUnit, String offset) {
        String retTime = NewDateUtils.millSecConvertToTimeStr(
                System.currentTimeMillis(), cycleUnit);
        try {
            long time = NewDateUtils.timeStrConvertTomillSec(dataTime, cycleUnit);

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time);
            Calendar retCalendar = getDateTime(calendar, cycleUnit, offset);
            if (retCalendar == null) {
                return dataTime;
            }

            retTime = NewDateUtils.millSecConvertToTimeStr(retCalendar.getTime().getTime(),
                    cycleUnit);
        } catch (Exception e) {
            logger.error("getDateTime error: ", e);
        }
        return retTime;
    }

    public static String getDateTime(long time, String cycleUnit, String offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        Calendar retCalendar = getDateTime(calendar, cycleUnit, offset);
        return NewDateUtils.millSecConvertToTimeStr(retCalendar.getTime().getTime(), cycleUnit);
    }

    private static Calendar getDateTime(Calendar calendar, String cycleUnit, String offset) {
        int cycleNumber = (cycleUnit.length() <= 1
                ? 1
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
            if ("D".equalsIgnoreCase(offsetUnit)) {
                calendar.add(Calendar.DAY_OF_YEAR, offsetNumber);
            }

            if ("H".equalsIgnoreCase(offsetUnit)) {
                calendar.add(Calendar.HOUR_OF_DAY, offsetNumber);
            }
        } else if (cycleUnit.length() == 1) {
            if ("D".equalsIgnoreCase(cycleUnit)) {
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
            } else if ("h".equalsIgnoreCase(cycleUnit)) {
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
            }
        }

        /* Calculate the offset. */
        if ("D".equalsIgnoreCase(offsetUnit)) {
            calendar.add(Calendar.DAY_OF_YEAR, offsetNumber);
        }

        if ("h".equalsIgnoreCase(offsetUnit)) {
            calendar.add(Calendar.HOUR_OF_DAY, offsetNumber);
        }

        if ("m".equals(offsetUnit)) {
            calendar.add(Calendar.MINUTE, offsetNumber);
        }

        return calendar;
    }

    public static boolean isValidCreationTime(String dataTime, String cycleUnit,
            String timeOffset) {
        long timeInterval = 0;
        if ("Y".equalsIgnoreCase(cycleUnit)) {
            timeInterval = DAY_TIMEOUT_INTERVAL;
        } else if ("M".equals(cycleUnit)) {
            timeInterval = HOUR_TIMEOUT_INTERVAL;
        } else if ("D".equalsIgnoreCase(cycleUnit)) {
            timeInterval = DAY_TIMEOUT_INTERVAL;
        } else if ("h".equalsIgnoreCase(cycleUnit)) {
            timeInterval = HOUR_TIMEOUT_INTERVAL;
        } else if (cycleUnit.contains("m")) {
            timeInterval = HOUR_TIMEOUT_INTERVAL;
        } else {
            logger.error("cycleUnit {} can't parse!", cycleUnit);
            timeInterval = DAY_TIMEOUT_INTERVAL;
        }

        // 处理偏移量，超时周期要加上时间偏移偏移量
        if (timeOffset.startsWith("-")) {
            timeInterval += caclOffset(timeOffset);
        } else { // 处理向后偏移
            timeInterval -= caclOffset(timeOffset);
        }

        return isValidCreationTime(dataTime, timeInterval);
    }

    /**
     * 根据偏移量计算偏移时间
     * 当前偏移只会向前偏移，也可向后偏移为兼容之前的计算方式(相减)，当为向后偏移时，返回负；当向前偏移，返回正
     *
     * @param timeOffset 偏移量，如-1d,-4h,-10m等；
     * @return
     */
    public static long caclOffset(String timeOffset) {
        String offsetUnit = timeOffset.substring(timeOffset.length() - 1);
        int startIndex = timeOffset.charAt(0) == '-' ? 1 : 0;
        // 默认向后偏移
        int symbol = 1;
        if (startIndex == 1) {
            symbol = 1;
        } else if (startIndex == 0) { // 向前偏移
            symbol = -1;
        }
        int offsetTime = Integer
                .parseInt(timeOffset.substring(startIndex, timeOffset.length() - 1));
        if ("d".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 24 * 3600 * 1000 * symbol;
        } else if ("h".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 3600 * 1000 * symbol;
        } else if ("m".equalsIgnoreCase(offsetUnit)) {
            return offsetTime * 60 * 1000 * symbol;
        }
        return 0;
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

    // convert millSec to YYYMMDD by cycleUnit
    public static String millSecConvertToTimeStr(long time, String cycleUnit, TimeZone tz) {
        String retTime = null;

        Calendar calendarInstance = Calendar.getInstance();
        calendarInstance.setTimeInMillis(time);

        Date dateTime = calendarInstance.getTime();
        SimpleDateFormat df = null;
        if ("Y".equalsIgnoreCase(cycleUnit)) {
            df = new SimpleDateFormat("yyyy");
        } else if ("M".equals(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMM");
        } else if ("D".equalsIgnoreCase(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMMdd");
        } else if ("h".equalsIgnoreCase(cycleUnit)) {
            df = new SimpleDateFormat("yyyyMMddHH");
        } else if (cycleUnit.contains("m")) {
            df = new SimpleDateFormat("yyyyMMddHHmm");
        } else {
            logger.error("cycleUnit {} can't parse!", cycleUnit);
            df = new SimpleDateFormat("yyyyMMddHH");
        }
        df.setTimeZone(tz);
        retTime = df.format(dateTime);

        if (cycleUnit.contains("m")) {

            int cycleNum = Integer.parseInt(cycleUnit.substring(0,
                    cycleUnit.length() - 1));
            int mmTime = Integer.parseInt(retTime.substring(
                    retTime.length() - 2, retTime.length()));
            String realMMTime = "";
            if (cycleNum * (mmTime / cycleNum) <= 0) {
                realMMTime = "0" + cycleNum * (mmTime / cycleNum);
            } else {
                realMMTime = "" + cycleNum * (mmTime / cycleNum);
            }
            retTime = retTime.substring(0, retTime.length() - 2) + realMMTime;
        }

        return retTime;
    }

    // convert millSec to YYYMMDD by cycleUnit
    public static String millSecConvertToTimeStr(long time, String cycleUnit) {
        return millSecConvertToTimeStr(time, cycleUnit, TimeZone.getDefault());
    }

    // convert YYYMMDD to millSec by cycleUnit
    public static long timeStrConvertTomillSec(String time, String cycleUnit)
            throws ParseException {
        return timeStrConvertTomillSec(time, cycleUnit, TimeZone.getDefault());
    }

    public static long timeStrConvertTomillSec(String time, String cycleUnit, TimeZone timeZone)
            throws ParseException {
        long retTime = 0;
        // SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat df = null;
        if (cycleUnit.equals("Y") && time.length() == 4) {
            df = new SimpleDateFormat("yyyy");
        } else if (cycleUnit.equals("M") && time.length() == 6) {
            df = new SimpleDateFormat("yyyyMM");
        } else if (cycleUnit.equals("D") && time.length() == 8) {
            df = new SimpleDateFormat("yyyyMMdd");
        } else if (cycleUnit.equalsIgnoreCase("h") && time.length() == 10) {
            df = new SimpleDateFormat("yyyyMMddHH");
        } else if (cycleUnit.contains("m") && time.length() == 12) {
            df = new SimpleDateFormat("yyyyMMddHHmm");
        } else {
            logger.error("time {},cycleUnit {} can't parse!", time, cycleUnit);
            throw new ParseException(time, 0);
        }
        try {
            df.setTimeZone(timeZone);
            retTime = df.parse(time).getTime();
            if (cycleUnit.equals("10m")) {

            }
        } catch (ParseException e) {
            logger.error("convert time string error. ", e);
        }
        return retTime;
    }

    public static boolean isBraceContain(String dataName) {
        Matcher matcher = bracePatt.matcher(dataName);
        return matcher.find();
    }

    public static String getDateTime(String fileName, String dataName,
            PathDateExpression dateExpression) {
        String dataTime = null;

        if (isBraceContain(dataName)) {
            String fullRegx = replaceDateExpressionWithRegex(dataName, "dataTime");
            Pattern fullPatt = Pattern.compile(fullRegx);
            Matcher matcher = fullPatt.matcher(fileName);
            if (matcher.find()) {
                dataTime = matcher.group("dataTime");
            }
        } else {
            dataTime = getDateTime(fileName, dateExpression);
        }

        return dataTime;

    }

    public static String getDateTime(String fileName, PathDateExpression dateExpression) {
        if (fileName == null || dateExpression == null
                || dateExpression.getLongestDatePattern() == null) {
            return null;
        }

        String longestDatePattern = NewDateUtils
                .replaceDateExpressionWithRegex(dateExpression.getLongestDatePattern());
        NonRegexPatternPosition patternPosition = dateExpression.getPatternPosition();

        Matcher mat = Pattern.compile(longestDatePattern).matcher(fileName);
        boolean find = mat.find();
        // TODO : 存在文件名中有多个部分匹配到时间表达式的情况("/data/joox_logs/2000701106/201602170040.log" YYYYMMDDhh)
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
            sb.append(dataPath.substring(lastIndex, m.getStart()));

            String longestPattern = m.getStr();
            int hhIndex = longestPattern.indexOf("hh");
            int mmIndex = longestPattern.indexOf("mm");
            longestPattern = longestPattern.replace("YYYY", "\\d{4}");
            longestPattern = longestPattern.replace("MM", "\\d{2}");
            longestPattern = longestPattern.replace("DD", "\\d{2}");
            longestPattern = longestPattern.replace("hh", "\\d{2}");

            if (hhIndex != -1 && mmIndex != -1
                    && mmIndex >= hhIndex + 2 && mmIndex < hhIndex + 4) {
                longestPattern = longestPattern.replace("mm", "\\d{2}");
            }
            sb.append(longestPattern);
            lastIndex = m.getEnd();
        }

        sb.append(dataPath.substring(lastIndex));

        return sb.toString();
    }

    public static String replaceDateExpressionWithRegex(String dataPath, String dateTimeGroupName) {
        if (dataPath == null) {
            return null;
        }

        // \\d{4}\\d{2}\\d{2}\\d{2} --> (?<GroupName>\\d{4}\\d{2}\\d{2}\\d{2})
        if (isBraceContain(dataPath)) {
            StringBuilder sb = new StringBuilder();
            sb.append(dataPath.substring(0, dataPath.indexOf('{')));
            sb.append("(?<").append(dateTimeGroupName).append('>');
            sb.append(dataPath.substring(dataPath.indexOf('{') + 1, dataPath.indexOf('}')));
            sb.append(')').append(dataPath.substring(dataPath.indexOf('}') + 1));
            dataPath = sb.toString();
        }

        StringBuffer sb = new StringBuffer();

        // find longest DATEPATTERN
        ArrayList<MatchPoint> mp = extractAllTimeRegex(dataPath);

        if (mp == null || mp.size() == 0) {
            return dataPath;
        }

        int lastIndex = 0;
        for (int i = 0; i < mp.size(); i++) {
            MatchPoint m = mp.get(i);
            sb.append(dataPath.substring(lastIndex, m.getStart()));

            String longestPattern = m.getStr();
            int hhIndex = longestPattern.indexOf("hh");
            int mmIndex = longestPattern.indexOf("mm");
            longestPattern = longestPattern.replace("YYYY", "\\d{4}");
            longestPattern = longestPattern.replace("MM", "\\d{2}");
            longestPattern = longestPattern.replace("DD", "\\d{2}");
            longestPattern = longestPattern.replace("hh", "\\d{2}");

            if (hhIndex != -1 && mmIndex != -1
                    && mmIndex >= hhIndex + 2 && mmIndex < hhIndex + 4) {
                longestPattern = longestPattern.replace("mm", "\\d{2}");
            }

            sb.append(longestPattern);
            lastIndex = m.getEnd();
        }

        sb.append(dataPath.substring(lastIndex));

        return sb.toString();
    }

    public static String replaceDateExpression(Calendar dateTime,
            String dataPath) {
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
            sb.append(dataPath.substring(lastIndex, m.getStart()));

            String longestPattern = m.getStr();
            int hhIndex = longestPattern.indexOf("hh");
            int mmIndex = longestPattern.indexOf("mm");

            longestPattern = longestPattern.replaceAll("YYYY", year);
            longestPattern = longestPattern.replaceAll("MM", externDate(month));
            longestPattern = longestPattern.replaceAll("DD", externDate(day));
            longestPattern = longestPattern.replaceAll("hh", externDate(hour));

            if (hhIndex != -1 && mmIndex != -1 && mmIndex >= hhIndex + 2
                    && mmIndex < hhIndex + 4) {
                longestPattern = longestPattern.replaceAll("mm", externDate(minute));
            }

            sb.append(longestPattern);
            lastIndex = m.getEnd();
        }

        sb.append(dataPath.substring(lastIndex));

        return sb.toString();
    }

    public static String replaceDateExpression1(Calendar dateTime,
            String logFileName) {
        if (dateTime == null || logFileName == null) {
            return null;
        }

        String year = String.valueOf(dateTime.get(Calendar.YEAR));
        String month = String.valueOf(dateTime.get(Calendar.MONTH) + 1);
        String day = String.valueOf(dateTime.get(Calendar.DAY_OF_MONTH));
        String hour = String.valueOf(dateTime.get(Calendar.HOUR_OF_DAY));
        String minute = String.valueOf(dateTime.get(Calendar.MINUTE));

        int hhIndex = logFileName.indexOf("hh");
        int mmIndex = logFileName.indexOf("mm");

        logFileName = logFileName.replaceAll("YYYY", year);
        logFileName = logFileName.replaceAll("MM", externDate(month));
        logFileName = logFileName.replaceAll("DD", externDate(day));
        logFileName = logFileName.replaceAll("hh", externDate(hour));

        if (hhIndex != -1 && mmIndex != -1 && mmIndex >= hhIndex + 2
                && mmIndex < hhIndex + 4) {
            logFileName = logFileName.replaceAll("mm", externDate(minute));
        }

        return logFileName;
    }

    private static String externDate(String time) {
        if (time.length() == 1) {
            return "0" + time;
        } else {
            return time;
        }
    }

    public static String parseCycleUnit(String scheduleTime) {
        String cycleUnit = "D";

        StringTokenizer st = new StringTokenizer(scheduleTime, " ");

        if (st.countTokens() <= 0) {
            return "D";
        }

        int index = 0;
        while (st.hasMoreElements()) {
            String currentString = st.nextToken();
            if (currentString.contains("/")) {
                if (index == 1) {
                    cycleUnit = "10m";
                } else if (index == 2) {
                    cycleUnit = "h";
                }
                break;
            }

            if (currentString.equals("*")) {
                if (index == 3) {
                    cycleUnit = "D";
                }
                break;
            }

            index++;
        }

        logger.info("ScheduleTime: " + scheduleTime + ", cycleUnit: "
                + cycleUnit);

        return cycleUnit;
    }

    // start: 20120810
    // end: 20120817
    // timeval: YYYYMMDDhh
    public static List<Long> getDateRegion(String start, String end,
            String cycleUnit) {
        // TODO : timeval verify

        List<Long> ret = new ArrayList<Long>();
        long startTime;
        long endTime;
        try {
            startTime = NewDateUtils.timeStrConvertTomillSec(start, cycleUnit);
            endTime = NewDateUtils.timeStrConvertTomillSec(end, cycleUnit);
        } catch (ParseException e) {
            logger.error("date format is error: ", e);
            return ret;
        }
        DateTime dtStart = DateTime.forInstant(startTime, TimeZone.getDefault());
        DateTime dtEnd = DateTime.forInstant(endTime, TimeZone.getDefault());

        if (cycleUnit.equals("M")) {
            dtEnd = dtEnd.getEndOfMonth();
        } else if (cycleUnit.equals("D")) {
            dtEnd = dtEnd.getEndOfDay();
        }

        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        if (cycleUnit.equalsIgnoreCase("Y")) {
            year = 1;
        } else if (cycleUnit.equals("M")) {
            month = 1;
        } else if (cycleUnit.equalsIgnoreCase("D")) {
            day = 1;
        } else if (cycleUnit.equalsIgnoreCase("h")) {
            hour = 1;
        } else if (cycleUnit.equals("10m")) {
            minute = 10;
        } else if (cycleUnit.equals("15m")) {
            minute = 15;
        } else if (cycleUnit.equals("30m")) {
            minute = 30;
        } else if (cycleUnit.equalsIgnoreCase("s")) {
            second = 1;
        } else {
            logger.error("cycelUnit {} is error: ", cycleUnit);
            return ret;
        }
        while (dtStart.lteq(dtEnd)) {
            ret.add(dtStart.getMilliseconds(TimeZone.getDefault()));
            dtStart = dtStart.plus(year, month, day, hour, minute, second, 0,
                    DateTime.DayOverflow.LastDay);
        }

        return ret;
    }

    public static void main(String[] args) throws Exception {

        // String aa = "/data/taox/YYYYMMDDt_log/[0-9]+_YYYYMMDD_hh00.log";
        /*
         * String aa = "/data/taox/YYYYt_logMMDD/[0-9]+_YYYYMMDD_hh00.log"; String bb =
         * replaceDateExpressionWithRegex(aa); System.out.println("---------: " + bb);
         *
         * String cc = replaceDateExpression(Calendar.getInstance(), aa); System.out.println("---------: " + cc);
         *
         * String dd = replaceDateExpression1(Calendar.getInstance(), aa); System.out.println("---------: " + dd);
         */

        // String text = "/data1/qq_BaseInfo/YYYY-MM/YYYY-MM-DD/gamedr.gamedb[0-9]+.minigame
        // .db/YYYY-MM-DD-[0-9]+.txt";
        // System.out.println(replaceDateExpressionWithRegex(text));
        //
        // int timeInterval = 1000;
        // String timeOffset = "10H";
        //
        // String offsetUnit = timeOffset.substring(timeOffset.length() - 1);
        // int startIndex = timeOffset.charAt(0) == '-' ? 1 : 0;
        // int offsetTime = Integer.parseInt(timeOffset.substring(startIndex, timeOffset.length()
        // - 1));
        // if("d".equalsIgnoreCase(offsetUnit)){
        // timeInterval += offsetTime * 24 * 3600 * 1000;
        // }else if("h".equalsIgnoreCase(offsetUnit)){
        // timeInterval += offsetTime * 3600 * 1000;
        // }else if("m".equalsIgnoreCase(offsetUnit)){
        // timeInterval += offsetTime * 60 * 1000;
        // }
        //
        // System.out.println(timeInterval);
        //
        // SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        //
        // Calendar calendar = NewDateUtils.getCurDate("D", "-10D");
        // System.out.println("year: " + calendar.get(Calendar.YEAR) + ", month: "
        // + (calendar.get(Calendar.MONTH) + 1) + ", day: "
        // + calendar.get(Calendar.DAY_OF_MONTH) + ", hour: "
        // + calendar.get(Calendar.HOUR_OF_DAY) + ", minute: "
        // + calendar.get(Calendar.MINUTE) + ", second: "
        // + calendar.get(Calendar.SECOND));
        // System.out.println(dateFormat.format(calendar.getTimeInMillis()));
        //
        // calendar = getCurDate("H", "-2H");
        // System.out.println("year: " + calendar.get(Calendar.YEAR) + ", month: "
        // + (calendar.get(Calendar.MONTH) + 1) + ", day: "
        // + calendar.get(Calendar.DAY_OF_MONTH) + ", hour: "
        // + calendar.get(Calendar.HOUR_OF_DAY) + ", minute: "
        // + calendar.get(Calendar.MINUTE) + ", second: "
        // + calendar.get(Calendar.SECOND));
        // System.out.println(dateFormat.format(calendar.getTimeInMillis()));
        //
        // calendar = getCurDate("H", "-2D");
        // System.out.println("year: " + calendar.get(Calendar.YEAR) + ", month: "
        // + (calendar.get(Calendar.MONTH) + 1) + ", day: "
        // + calendar.get(Calendar.DAY_OF_MONTH) + ", hour: "
        // + calendar.get(Calendar.HOUR_OF_DAY) + ", minute: "
        // + calendar.get(Calendar.MINUTE) + ", second: "
        // + calendar.get(Calendar.SECOND));
        // System.out.println(dateFormat.format(calendar.getTimeInMillis()));
        //
        // calendar = getCurDate("5m", "-20m");
        // System.out.println("year: " + calendar.get(Calendar.YEAR) + ", month: "
        // + (calendar.get(Calendar.MONTH) + 1) + ", day: "
        // + calendar.get(Calendar.DAY_OF_MONTH) + ", hour: "
        // + calendar.get(Calendar.HOUR_OF_DAY) + ", minute: "
        // + calendar.get(Calendar.MINUTE) + ", second: "
        // + calendar.get(Calendar.SECOND));
        // System.out.println(dateFormat.format(calendar.getTimeInMillis()));
        //
        // String directory = "/data/home/user00/xyshome/logsvr/log/YYYYMMDD/[0-9]+_YYYYMMDD_hh00
        // .log";
        // calendar = getCurDate("H", "-3H");
        // System.out.println(replaceDateExpression(calendar, directory));
        //
        // System.out.println(NewDateUtils.timeStrConvertTomillSec("201404031105",
        // "m"));
        // System.out.println(NewDateUtils.timeStrConvertTomillSec("2014040223",
        // "H"));
        // System.out.println(NewDateUtils
        // .timeStrConvertTomillSec("20140402", "D"));
        //
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "Y"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "M"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "D"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "H"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "10m"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "15m"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // System.currentTimeMillis(), "30m"));
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(
        // NewDateUtils.timeStrConvertTomillSec("201404121900", "10m"),
        // "10m"));
        //
        // NewDateUtils.getDateRegion("20120810", "20120813", "D");
        // NewDateUtils.getDateRegion("2012081005", "2012081300", "H");
        // NewDateUtils.getDateRegion("201404111649", "201404111600", "10m");
        // String dataTime = "20160122";
        // System.out.println(NewDateUtils.getShouldStartTime(dataTime, "D", "-2h"));

        // String dataPath = "/data/herococo/YYYYMMDD_*/YYYYMMDDhhmm.log";
        // dataPath = NewDateUtils.replaceDateExpressionWithRegex(dataPath);
        // System.out.println("dataPath: " + dataPath);
        //
        // Pattern pattern = Pattern.compile(dataPath, Pattern.CASE_INSENSITIVE
        // | Pattern.DOTALL | Pattern.MULTILINE);
        // // Pattern pattern = Pattern.compile("/data/herococo/\\d+/\\d+.log",
        // // Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        // Matcher m = pattern
        // .matcher("/data/herococo/20140406_a/20140406152730.log");
        // System.out.println(m.matches());
        //
        // dataPath = "/data/home/user00/xyshome/logsvr/log/YYYYMMDD/[0-9]+_YYYYMMDD_hh00.log";
        // dataPath = NewDateUtils.replaceDateExpressionWithRegex(dataPath);
        // pattern = Pattern.compile(dataPath, Pattern.CASE_INSENSITIVE
        // | Pattern.DOTALL | Pattern.MULTILINE);
        // m = pattern
        // .matcher("/data/home/user00/xyshome/logsvr/log/20140406/8_20140406_1600.log");
        // System.out.println(dataPath);
        // System.out.println(m.matches());
        //
        // dataPath = "/data/work/data2/abc/YYYYMMDDhh.*.txt";
        // dataPath = NewDateUtils.replaceDateExpressionWithRegex(dataPath);
        // pattern = Pattern.compile(dataPath, Pattern.CASE_INSENSITIVE
        // | Pattern.DOTALL | Pattern.MULTILINE);
        // m = pattern.matcher("/data/work/data2/abc/201404102242.txt");
        // System.out.println(dataPath);
        // System.out.println(m.matches());
        //
        // List<Long> retTimeList = NewDateUtils.getDateRegion("20140411",
        // "20140411", "D");
        // for (Long time : retTimeList) {
        // System.out.println(NewDateUtils.millSecConvertToTimeStr(time, "D"));
        // }
        //
        // pattern = Pattern
        // .compile(
        // "/data/home/tlog/logplat/log/tlogd_1/[0-9]+_\\d{4}\\d{2}\\d{2}_\\d{2}00
        // .log",
        // Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        // | Pattern.MULTILINE);
        // m = pattern
        // .matcher("/data/home/tlog/logplat/log/tlogd_1/65535_20140506_1600.log.1");
        // System.out.println(m.matches());
        // System.out.println(m.lookingAt());
        //
        // String unit = "h";
        // if (StringUtils.endsWithIgnoreCase("h", "H")) {
        // System.out.println("yes");
        // }
        //
        // System.out.println(NewDateUtils.getDateTime("20160106", "D", "-4h"));

        // PathDateExpression dateExpression = DateUtils
        // .extractLongestTimeRegexWithPrefixOrSuffix
        // ("/data/log/qqtalk/[0-9]+_[0-9]+_id20522_[0-9]+_YYYYMMDD_hh.log");
        // System.out.println(dateExpression.getLongestDatePattern());
        // String fileTime = getDateTime("/data/log/qqtalk/3900626911_11217_id20522_17_20160420
        // .log", dateExpression);
        // System.out.println(fileTime);

        // String dataTime = "20180411";
        //
        // String shouldStart = getShouldStartTime(dataTime, "D", "4h");
        // System.out.println(shouldStart);
        //
        // String fileName = "rc_trade_water[0-9]*.YYYY-MM-DD-hh.[0-9]+";
        //
        // String newFileName = "rc_trade_water.2016-11-20-12.9";
        //
        // /**
        // * 打印出文件名中 最长的时间表达式
        // */
        // PathDateExpression dateExpression = DateUtils.extractLongestTimeRegexWithPrefixOrSuffix
        // (fileName);
        // System.out.println(dateExpression.getLongestDatePattern());
        //
        // /**
        // * 检查正则表达式是否能匹配到文件
        // */
        // Pattern pattern = Pattern.compile(replaceDateExpressionWithRegex(fileName),
        // Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        //
        // Matcher matcher = pattern.matcher(newFileName);
        //
        // if (matcher.matches() || matcher.lookingAt()) {
        // System.out.println("Matched File");
        // }
        //
        // /**
        // * 打印文件名的时间
        // */
        // String fileTime = getDateTime(newFileName, dateExpression);
        // System.out.println(fileTime);
        //
        //
        // String fileName1 = "/data/joox_logs/2000701106/201602170040.log";
        // String filePathRegx = "/data/joox_logs/2000701106/{YYYYMMDDhh}40.log";
        // String fullRegx = replaceDateExpressionWithRegex(filePathRegx, "dateTimeGN");
        // System.out.println(fullRegx);
        // Pattern fullPattern = Pattern.compile(fullRegx);
        // Matcher fullMatcher = fullPattern.matcher(fileName1);
        // while (fullMatcher.find()) {
        // System.out.println(fullMatcher.group("dateTimeGN"));
        // }

        System.out.println(
                timeStrConvertTomillSec("2018111209", "h", TimeZone.getTimeZone("GMT+8:00")));
    }
}
