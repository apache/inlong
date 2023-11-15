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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateTransUtils {

    private static final Logger logger = LoggerFactory.getLogger(DateTransUtils.class);

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

}
