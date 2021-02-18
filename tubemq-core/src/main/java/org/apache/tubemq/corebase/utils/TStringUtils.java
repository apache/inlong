/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified from commons-lang <a href="https://github.com/apache/commons-lang"> Project</a>
 *   file address: https://github.com/apache/commons-lang/blob/LANG_2_X/src/
 *                         main/java/org/apache/commons/lang/StringUtils.java
 */

package org.apache.tubemq.corebase.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;

/**
 * Utility to String operations.
 */
public class TStringUtils {

    public static final String EMPTY = "";

    // Empty checks
    //-----------------------------------------------------------------------
    /**
     * <p>Checks if a String is empty ("") or null.</p>
     *
     * <pre>
     * TStringUtils.isEmpty(null)      = true
     * TStringUtils.isEmpty("")        = true
     * TStringUtils.isEmpty(" ")       = false
     * TStringUtils.isEmpty("bob")     = false
     * TStringUtils.isEmpty("  bob  ") = false
     * </pre>
     *
     * <p>NOTE: This method changed in Lang version 2.0.
     * It no longer trims the String.
     * That functionality is available in isBlank().</p>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if the String is empty or null
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * <p>Checks if a String is not empty ("") and not null.</p>
     *
     * <pre>
     * TStringUtils.isNotEmpty(null)      = false
     * TStringUtils.isNotEmpty("")        = false
     * TStringUtils.isNotEmpty(" ")       = true
     * TStringUtils.isNotEmpty("bob")     = true
     * TStringUtils.isNotEmpty("  bob  ") = true
     * </pre>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if the String is not empty and not null
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * <p>Checks if a String is whitespace, empty ("") or null.</p>
     *
     * <pre>
     * TStringUtils.isBlank(null)      = true
     * TStringUtils.isBlank("")        = true
     * TStringUtils.isBlank(" ")       = true
     * TStringUtils.isBlank("bob")     = false
     * TStringUtils.isBlank("  bob  ") = false
     * </pre>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     * @since 2.0
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>Checks if a String is not empty (""), not null and not whitespace only.</p>
     *
     * <pre>
     * TStringUtils.isNotBlank(null)      = false
     * TStringUtils.isNotBlank("")        = false
     * TStringUtils.isNotBlank(" ")       = false
     * TStringUtils.isNotBlank("bob")     = true
     * TStringUtils.isNotBlank("  bob  ") = true
     * </pre>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if the String is
     *  not empty and not null and not whitespace
     * @since 2.0
     */
    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    /**
     * <p>Removes control characters (char &lt;= 32) from both
     * ends of this String, handling <code>null</code> by returning
     * <code>null</code>.</p>
     *
     * <p>The String is trimmed using {@link String#trim()}.
     * Trim removes start and end characters &lt;= 32.</p>
     *
     * <pre>
     * TStringUtils.trim(null)          = null
     * TStringUtils.trim("")            = ""
     * TStringUtils.trim("     ")       = ""
     * TStringUtils.trim("abc")         = "abc"
     * TStringUtils.trim("    abc    ") = "abc"
     * </pre>
     *
     * @param str  the String to be trimmed, may be null
     * @return the trimmed string, <code>null</code> if null String input
     */
    public static String trim(String str) {
        return str == null ? null : str.trim();
    }

    // Misc
    //-----------------------------------------------------------------------
    /**
     * <p>Find the Levenshtein distance between two Strings.</p>
     *
     * <p>This is the number of changes needed to change one String into
     * another, where each change is a single character modification (deletion,
     * insertion or substitution).</p>
     *
     * <p>The previous implementation of the Levenshtein distance algorithm
     * was from <a href="http://www.merriampark.com/ld.htm">http://www.merriampark.com/ld.htm</a></p>
     *
     * <p>Chas Emerick has written an implementation in Java, which avoids an OutOfMemoryError
     * which can occur when my Java implementation is used with very large strings.<br>
     * This implementation of the Levenshtein distance algorithm
     * is from <a href="http://www.merriampark.com/ldjava.htm">http://www.merriampark.com/ldjava.htm</a></p>
     *
     * <pre>
     * TStringUtils.getLevenshteinDistance(null, *)             = IllegalArgumentException
     * TStringUtils.getLevenshteinDistance(*, null)             = IllegalArgumentException
     * TStringUtils.getLevenshteinDistance("","")               = 0
     * TStringUtils.getLevenshteinDistance("","a")              = 1
     * TStringUtils.getLevenshteinDistance("aaapppp", "")       = 7
     * TStringUtils.getLevenshteinDistance("frog", "fog")       = 1
     * TStringUtils.getLevenshteinDistance("fly", "ant")        = 3
     * TStringUtils.getLevenshteinDistance("elephant", "hippo") = 7
     * TStringUtils.getLevenshteinDistance("hippo", "elephant") = 7
     * TStringUtils.getLevenshteinDistance("hippo", "zzzzzzzz") = 8
     * TStringUtils.getLevenshteinDistance("hello", "hallo")    = 1
     * </pre>
     *
     * @param s  the first String, must not be null
     * @param t  the second String, must not be null
     * @return result distance
     * @throws IllegalArgumentException if either String input <code>null</code>
     */
    public static int getLevenshteinDistance(String s, String t) {
        if (s == null || t == null) {
            throw new IllegalArgumentException("Strings must not be null");
        }

        /*
           The difference between this impl. and the previous is that, rather
           than creating and retaining a matrix of size s.length()+1 by t.length()+1,
           we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
           is the 'current working' distance array that maintains the newest distance cost
           counts as we iterate through the characters of String s.  Each time we increment
           the index of String t we are comparing, d is copied to p, the second int[].  Doing so
           allows us to retain the previous cost counts as required by the algorithm (taking
           the minimum of the cost count to the left, up one, and diagonally up and to the left
           of the current cost count being calculated).  (Note that the arrays aren't really
           copied anymore, just switched...this is clearly much better than cloning an array
           or doing a System.arraycopy() each time  through the outer loop.)
           Effectively, the difference between the two implementations is this one does not
           cause an out of memory condition when calculating the LD over two very large strings.
         */

        int n = s.length(); // length of s
        int m = t.length(); // length of t

        if (n == 0) {
            return m;
        } else if (m == 0) {
            return n;
        }

        if (n > m) {
            // swap the input strings to consume less memory
            String tmp = s;
            s = t;
            t = tmp;
            n = m;
            m = t.length();
        }

        int p[] = new int[n + 1]; //'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int swap[]; //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char chkChar; // jth character of t

        int cost; // cost

        for (i = 0; i <= n; i++) {
            p[i] = i;
        }

        for (j = 1; j <= m; j++) {
            chkChar = t.charAt(j - 1);
            d[0] = j;

            for (i = 1; i <= n; i++) {
                cost = s.charAt(i - 1) == chkChar ? 0 : 1;
                // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1),  p[i - 1] + cost);
            }

            // copy current distance counts to 'previous row' distance counts
            swap = p;
            p = d;
            d = swap;
        }

        // our last action in the above loop was to switch d and p, so p now
        // actually has the most recent cost counts
        return p[n];
    }

    public static boolean isLetter(char ch) {
        return (Character.isUpperCase(ch)
                || Character.isLowerCase(ch));
    }

    public static boolean isLetterOrDigit(char ch) {
        return isLetter(ch) || Character.isDigit(ch);
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toCamelCase(null)  = null
     * TStringUtils.toCamelCase("")    = ""
     * TStringUtils.toCamelCase("aBc") = "aBc"
     * TStringUtils.toCamelCase("aBc def") = "aBcDef"
     * TStringUtils.toCamelCase("aBc def_ghi") = "aBcDefGhi"
     * TStringUtils.toCamelCase("aBc def_ghi 123") = "aBcDefGhi123"
     * </pre>
     * <p/>
     * </p> <p> This method preserves all separators except underscores and whitespace. </p>
     *
     * @param origStr The string to be converted
     * @return Convert the string to Camel Case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toCamelCase(String origStr) {
        if (isEmpty(origStr)) {
            return origStr;
        }
        origStr = origStr.trim();
        int length = origStr.length();

        char curChar;
        char preChar;
        int curWritePos = 0;
        boolean upperCaseNext = false;
        char[] tgtStr = new char[length];
        for (int index = 0; index < length; ) {
            curChar = origStr.charAt(index);
            index += Character.charCount(curChar);
            // ignore white space chars
            if (Character.isWhitespace(curChar)) {
                continue;
            }
            // process char and '_' delimiter
            if (isLetter(curChar)) {
                if (upperCaseNext) {
                    upperCaseNext = false;
                    curChar = Character.toUpperCase(curChar);
                } else {
                    if (curWritePos == 0) {
                        curChar = Character.toLowerCase(curChar);
                    } else {
                        preChar = tgtStr[curWritePos - 1];
                        // judge pre-read char not Letter or digit
                        if (!isLetterOrDigit(preChar)) {
                            curChar = Character.toLowerCase(curChar);
                        } else {
                            if (Character.isUpperCase(preChar)) {
                                curChar = Character.toLowerCase(curChar);
                            }
                        }
                    }
                }
                tgtStr[curWritePos++] = curChar;
            } else {
                if (curChar == '_') {
                    upperCaseNext = true;
                } else {
                    tgtStr[curWritePos++] = curChar;
                }
            }
        }
        return new String(tgtStr, 0, curWritePos);
    }

    public static String getAuthSignature(final String usrName,
                                          final String usrPassWord,
                                          long timestamp, int randomValue) {
        Base64 base64 = new Base64();
        StringBuilder sbuf = new StringBuilder(512);
        byte[] baseStr =
                base64.encode(HmacUtils.hmacSha1(usrPassWord,
                        sbuf.append(usrName).append(timestamp)
                                .append(randomValue).toString()));
        sbuf.delete(0, sbuf.length());
        String signature = "";
        try {
            signature = URLEncoder.encode(new String(baseStr,
                            TBaseConstants.META_DEFAULT_CHARSET_NAME),
                    TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return signature;
    }

    public static String setAttrValToAttributes(String srcAttrs,
                                                String attrKey, String attrVal) {
        StringBuilder sbuf = new StringBuilder(512);
        if (isBlank(srcAttrs)) {
            return sbuf.append(attrKey).append(TokenConstants.EQ).append(attrVal).toString();
        }
        if (!srcAttrs.contains(attrKey)) {
            return sbuf.append(srcAttrs)
                    .append(TokenConstants.SEGMENT_SEP)
                    .append(attrKey).append(TokenConstants.EQ).append(attrVal).toString();
        }
        boolean notFirst = false;
        String[] strAttrs = srcAttrs.split(TokenConstants.SEGMENT_SEP);
        for (String strAttrItem : strAttrs) {
            if (isNotBlank(strAttrItem)) {
                if (notFirst) {
                    sbuf.append(TokenConstants.SEGMENT_SEP);
                }
                if (strAttrItem.contains(attrKey)) {
                    sbuf.append(attrKey).append(TokenConstants.EQ).append(attrVal);
                } else {
                    sbuf.append(strAttrItem);
                }
                notFirst = true;
            }
        }
        return sbuf.toString();
    }

    public static String getAttrValFrmAttributes(String srcAttrs, String attrKey) {
        if (!isBlank(srcAttrs)) {
            String[] strAttrs = srcAttrs.split(TokenConstants.SEGMENT_SEP);
            for (String attrItem : strAttrs) {
                if (isNotBlank(attrItem)) {
                    String[] kv = attrItem.split(TokenConstants.EQ);
                    if (attrKey.equals(kv[0])) {
                        return kv[1];
                    }
                }
            }
        }
        return null;
    }

}
