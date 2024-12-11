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

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;

public class PatternUtil {

    private static final String YEAR = "YYYY";
    private static final String MONTH = "MM";
    private static final String DAY = "DD";
    private static final String HOUR = "hh";

    public static ArrayList<String> cutDirectoryByWildcard(String directory) {
        String baseDirectory;
        String regixDirecotry;
        String fileName;

        File file = new File(directory);
        fileName = file.getName();

        int fileNameIndex = directory.length() - fileName.length() - 1;
        String sign = "\\^$*+?{(|[.";
        String range = "+?*{";

        int regixDirecotryIndex = StringUtils.indexOfAny(directory, sign);
        if (regixDirecotryIndex != -1
                && directory.charAt(regixDirecotryIndex) == '.') {
            if (regixDirecotryIndex != directory.length() - 1) {
                char c = directory.charAt(regixDirecotryIndex + 1);
                if (StringUtils.indexOf(range, c) == -1) {
                    regixDirecotryIndex = StringUtils.indexOfAny(directory,
                            sign.substring(0, sign.length() - 1));
                }
            }
        }
        if (regixDirecotryIndex < fileNameIndex) {
            int regixDirecotryBeginIndex = directory.lastIndexOf('/',
                    regixDirecotryIndex);
            if (regixDirecotryBeginIndex == -1) {
                baseDirectory = directory.substring(0, fileNameIndex);
                regixDirecotry = "";
            } else {

                regixDirecotry = directory.substring(
                        regixDirecotryBeginIndex + 1, fileNameIndex);
                if (regixDirecotryBeginIndex == 0) {
                    baseDirectory = "/";
                } else {
                    baseDirectory = directory.substring(0,
                            regixDirecotryBeginIndex);
                }
            }
        } else {
            baseDirectory = directory.substring(0, fileNameIndex);
            regixDirecotry = "";
        }

        ArrayList<String> ret = new ArrayList<String>();
        ret.add(baseDirectory);
        ret.add(regixDirecotry);
        ret.add(fileName);
        return ret;
    }

    public static ArrayList<String> cutDirectoryByWildcardAndDateExpression(String directory) {
        String baseDirectory;
        String regixDirectory;
        String fileName;

        File file = new File(directory);
        fileName = file.getName();

        int fileNameIndex = directory.length() - fileName.length() - 1;
        String sign = "\\^$*+?{(|[.";

        String range = "+?*{";

        int regixDirecotryIndex = StringUtils.indexOfAny(directory, sign);
        if (regixDirecotryIndex != -1
                && directory.charAt(regixDirecotryIndex) == '.') {
            if (regixDirecotryIndex != directory.length() - 1) {
                char c = directory.charAt(regixDirecotryIndex + 1);
                if (StringUtils.indexOf(range, c) == -1) {
                    regixDirecotryIndex = StringUtils.indexOfAny(directory,
                            sign.substring(0, sign.length() - 1));
                }
            }
        }
        if (regixDirecotryIndex < fileNameIndex) {
            int regixDirecotryBeginIndex = directory.lastIndexOf('/',
                    regixDirecotryIndex);
            if (regixDirecotryBeginIndex == -1) {
                baseDirectory = directory.substring(0, fileNameIndex);
                regixDirectory = "";
            } else {
                regixDirectory = directory.substring(
                        regixDirecotryBeginIndex + 1, fileNameIndex);
                if (regixDirecotryBeginIndex == 0) {
                    baseDirectory = "/";
                } else {
                    baseDirectory = directory.substring(0,
                            regixDirecotryBeginIndex);
                }
            }
        } else {
            baseDirectory = directory.substring(0, fileNameIndex);
            regixDirectory = "";
        }

        int[] indexes = new int[]{
                (baseDirectory.contains(YEAR) ? baseDirectory.indexOf(YEAR) : Integer.MAX_VALUE),
                (baseDirectory.contains(MONTH) ? baseDirectory.indexOf(MONTH) : Integer.MAX_VALUE),
                (baseDirectory.contains(DAY) ? baseDirectory.indexOf(DAY) : Integer.MAX_VALUE),
                (baseDirectory.contains(HOUR) ? baseDirectory.indexOf(HOUR) : Integer.MAX_VALUE)};

        int minIndex = Integer.MAX_VALUE;
        for (int i = 0; i < indexes.length; i++) {
            if (minIndex > indexes[i]) {
                minIndex = indexes[i];
            }
        }

        if (minIndex != Integer.MAX_VALUE) {
            int lastIndex = baseDirectory.lastIndexOf('/', minIndex);
            if (regixDirectory.length() > 0) {
                regixDirectory = baseDirectory.substring(lastIndex + 1, baseDirectory.length())
                        + File.separator + regixDirectory;
            } else {
                regixDirectory = baseDirectory.substring(lastIndex + 1, baseDirectory.length());
            }
            baseDirectory = baseDirectory.substring(0, lastIndex);
        }

        ArrayList<String> ret = new ArrayList<String>();
        ret.add(baseDirectory);
        ret.add(regixDirectory);
        ret.add(fileName);
        return ret;
    }

    public static boolean isSameDir(String fileName1, String fileName2) {
        ArrayList<String> ret1 = PatternUtil.cutDirectoryByWildcard(fileName1);
        ArrayList<String> ret2 = PatternUtil.cutDirectoryByWildcard(fileName2);
        return ret1.get(0).equals(ret2.get(0));
    }

    public static String getBeforeFirstWildcard(String input) {
        String sign = "\\^$*+?{(|[.";
        int firstWildcardIndex = StringUtils.indexOfAny(input, sign);
        if (firstWildcardIndex != -1) {
            return input.substring(0, firstWildcardIndex);
        } else {
            return "";
        }
    }
}
