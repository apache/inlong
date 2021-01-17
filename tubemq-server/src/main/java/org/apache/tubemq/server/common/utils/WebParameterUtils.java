/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.common.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.servlet.http.HttpServletRequest;
import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.TStatusConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerConfManager;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerSyncStatusInfo;



public class WebParameterUtils {

    private static final List<String> allowedDelUnits = Arrays.asList("s", "m", "h");

    /**
     * Parse the parameter value from an object value to a long value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a long value of parameter
     * @throws Exception if failed to parse the object
     */
    public static long validLongDataParameter(String paramName, String paramValue,
                                              boolean required, long defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_NUMBER_VALUE)) {
            throw new Exception(new StringBuilder(512).append("the value of ")
                    .append(paramName).append(" parameter must only contain numbers").toString());
        }
        return Long.parseLong(tmpParamValue);
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a int value of parameter
     * @throws Exception if failed to parse the object
     */
    public static int validIntDataParameter(String paramName, String paramValue,
                                            boolean required, int defaultValue,
                                            int minValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_NUMBER_VALUE)) {
            throw new Exception(new StringBuilder(512)
                    .append("the value of ").append(paramName)
                    .append(" parameter must only contain numbers").toString());
        }
        int tmpInteger = Integer.parseInt(tmpParamValue);
        if (tmpInteger < minValue) {
            throw new Exception(new StringBuilder(512)
                    .append("the value of ").append(paramName)
                    .append(" parameter must >= ").append(minValue).toString());
        }
        return tmpInteger;
    }

    /**
     * Parse the parameter value from an object value to a boolean value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a boolean value of parameter
     * @throws Exception if failed to parse the object
     */
    public static boolean validBooleanDataParameter(String paramName, String paramValue,
                                                    boolean required, boolean defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(tmpParamValue);
    }

    /**
     * Parse the parameter value from an object value to Date value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a Date value of parameter
     * @throws Exception if failed to parse the object
     */
    public static Date validDateParameter(String paramName, String paramValue, int paramMaxLen,
                                          boolean required, Date defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        // yyyyMMddHHmmss
        if (tmpParamValue.length() > paramMaxLen) {
            throw new Exception(new StringBuilder(512)
                    .append("the date format is yyyyMMddHHmmss of ")
                    .append(paramName).append(" parameter").toString());
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_NUMBER_VALUE)) {
            throw new Exception(new StringBuilder(512).append("the value of ")
                    .append(paramName).append(" parameter must only contain numbers").toString());
        }
        DateFormat sdf = new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
        Date date = sdf.parse(tmpParamValue);
        return date;
    }

    /**
     * Parse the parameter value from an object value to string value
     *
     * @param req          http servlet request
     * @param paramName    the parameter name
     * @param paramMaxLen  the max length of string to return
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a string value of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validStringParameter(HttpServletRequest req, String paramName,
                                              int paramMaxLen, boolean required,
                                              String defaultValue) throws Exception {
        return validStringParameter(paramName,
                req.getParameter(paramName), paramMaxLen, required, defaultValue);
    }

    /**
     * Parse the parameter value from an object value to string value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param paramMaxLen  the max length of string to return
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a string value of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validStringParameter(String paramName, String paramValue,
                                              int paramMaxLen, boolean required,
                                              String defaultValue) throws Exception {
        String tmpParamValue =
                checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        if (paramMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (tmpParamValue.length() > paramMaxLen) {
                throw new Exception(new StringBuilder(512).append("the max length of ")
                        .append(paramName).append(" parameter is ")
                        .append(paramMaxLen).append(" characters").toString());
            }
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_STRING_VALUE)) {
            throw new Exception(new StringBuilder(512).append("the value of ")
                    .append(paramName).append(" parameter must begin with a letter, ")
                    .append("can only contain characters,numbers,and underscores").toString());
        }
        return tmpParamValue;
    }

    /**
     * Parse the parameter value from an object value to group string value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param paramMaxLen  the max length of string to return
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a string value of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validGroupParameter(String paramName, String paramValue, int paramMaxLen,
                                             boolean required, String defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        if (paramMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (tmpParamValue.length() > paramMaxLen) {
                throw new Exception(new StringBuilder(512).append("the max length of ")
                    .append(paramName).append(" parameter is ")
                    .append(paramMaxLen).append(" characters").toString());
            }
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_GROUP_VALUE)) {
            throw new Exception(new StringBuilder(512).append("the value of ")
                .append(paramName).append(" parameter must begin with a letter, ")
                .append("can only contain characters,numbers,hyphen,and underscores").toString());
        }
        return tmpParamValue;
    }

    public static StringBuilder buildFailResult(StringBuilder strBuffer, String errMsg) {
        return strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                .append(errMsg).append("\"}");
    }

    public static StringBuilder buildSuccessResult(StringBuilder strBuffer) {
        return strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
    }

    public static StringBuilder buildSuccessResult(StringBuilder strBuffer, String appendInfo) {
        return strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"").
                append(appendInfo).append("\"}");
    }
    /**
     * Parse the parameter value from an object value to a long value
     *
     * @param req        Http Servlet Request
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param result     process result of parameter value
     * @return process result
     */
    public static boolean getLongParamValue(HttpServletRequest req,
                                            WebFieldDef fieldDef,
                                            boolean required,
                                            long defValue,
                                            ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, result)) {
            return result.success;
        }
        String paramValue = (String) result.retData1;
        if (paramValue == null) {
            result.setSuccResult(defValue);
            return result.success;
        }
        try {
            long paramIntVal = Long.parseLong(paramValue);
            result.setSuccResult(paramIntVal);
        } catch (Throwable e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param req        Http Servlet Request
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param result     process result of parameter value
     * @return process result
     */
    public static boolean getIntParamValue(HttpServletRequest req,
                                           WebFieldDef fieldDef,
                                           boolean required,
                                           ProcessResult result) {
        return getIntParamValue(req, fieldDef, required,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                result);
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param req        Http Servlet Request
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param minValue   min value required
     * @param result     process result of parameter value
     * @return process result
     */
    public static boolean getIntParamValue(HttpServletRequest req,
                                           WebFieldDef fieldDef,
                                           boolean required,
                                           int defValue,
                                           int minValue,
                                           ProcessResult result) {
        return getIntParamValue(req, fieldDef, required, true, defValue,
                true, minValue, false, TBaseConstants.META_VALUE_UNDEFINED, result);
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param req        Http Servlet Request
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param minValue   min value required
     * @param minValue   max value allowed
     * @param result     process result of parameter value
     * @return process result
     */
    public static boolean getIntParamValue(HttpServletRequest req,
                                           WebFieldDef fieldDef,
                                           boolean required,
                                           int defValue,
                                           int minValue,
                                           int maxValue,
                                           ProcessResult result) {
        return getIntParamValue(req, fieldDef, required, true, defValue,
                true, minValue, true, maxValue, result);
    }

    private static boolean getIntParamValue(HttpServletRequest req,
                                            WebFieldDef fieldDef,
                                            boolean required,
                                            boolean hasDefVal,
                                            int defValue,
                                            boolean hasMinVal,
                                            int minValue,
                                            boolean hasMaxVal,
                                            int maxValue,
                                            ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, result)) {
            return result.success;
        }
        if (fieldDef.isCompFieldType()) {
            Set<Integer> tgtValueSet = new HashSet<Integer>();
            Set<String> valItemSet = (Set<String>) result.retData1;
            if (valItemSet.isEmpty()) {
                if (hasDefVal) {
                    tgtValueSet.add(defValue);
                }
                result.setSuccResult(tgtValueSet);
                return result.success;
            }
            for (String itemVal : valItemSet) {
                if (!checkIntValueNorms(fieldDef, itemVal,
                        hasMinVal, minValue, hasMaxVal, maxValue, result)) {
                    return result.success;
                }
                tgtValueSet.add((Integer) result.retData1);
            }
            result.setSuccResult(tgtValueSet);
        } else {
            String paramValue = (String) result.retData1;
            if (paramValue == null) {
                if (hasDefVal) {
                    result.setSuccResult(defValue);
                }
                return result.success;
            }
            checkIntValueNorms(fieldDef, paramValue,
                    hasMinVal, minValue, hasMaxVal, maxValue, result);
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an object value to a boolean value
     *
     * @param req         Http Servlet Request
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static boolean getBooleanParamValue(HttpServletRequest req,
                                               WebFieldDef fieldDef,
                                               boolean required,
                                               boolean defValue,
                                               ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, result)) {
            return result.success;
        }
        String paramValue = (String) result.retData1;
        if (paramValue == null) {
            result.setSuccResult(defValue);
            return result.success;
        }
        result.setSuccResult(Boolean.parseBoolean(paramValue));
        return result.success;
    }

    /**
     * Parse the parameter value from an object value
     *
     * @param req         Http Servlet Request
     * @param fieldDef    the parameter field definition
     * @param required     a boolean value represent whether the parameter is must required
     * @param defValue     a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static boolean getStringParamValue(HttpServletRequest req,
                                              WebFieldDef fieldDef,
                                              boolean required,
                                              String defValue,
                                              ProcessResult result) {
        // get parameter value
        String paramValue = req.getParameter(fieldDef.name);
        if (paramValue == null) {
            paramValue = req.getParameter(fieldDef.shortName);
        }
        if (TStringUtils.isNotBlank(paramValue)) {
            // Cleanup value extra characters
            paramValue = escDoubleQuotes(paramValue.trim());
        }
        // Check if the parameter exists
        if (TStringUtils.isBlank(paramValue)) {
            if (required) {
                result.setFailResult(new StringBuilder(512)
                        .append("Parameter ").append(fieldDef.name)
                        .append(" is missing or value is null or blank!").toString());
            } else {
                procStringDefValue(fieldDef.isCompFieldType(), defValue, result);
            }
            return result.success;
        }
        // check if value is norm;
        if (fieldDef.isCompFieldType()) {
            // split original value to items
            Set<String> valItemSet = new HashSet<>();
            String[] strParamValueItems = paramValue.split(fieldDef.splitToken);
            for (String strParamValueItem : strParamValueItems) {
                if (TStringUtils.isBlank(strParamValueItem)) {
                    continue;
                }
                if (!checkStrValueNorms(fieldDef, strParamValueItem, result)) {
                    return result.success;
                }
                valItemSet.add((String) result.retData1);
            }
            // check if is empty result
            if (valItemSet.isEmpty()) {
                if (required) {
                    result.setFailResult(new StringBuilder(512)
                            .append("Parameter ").append(fieldDef.name)
                            .append(" is missing or value is null or blank!").toString());
                } else {
                    procStringDefValue(fieldDef.isCompFieldType(), defValue, result);
                }
                return result.success;
            }
            // check max item count
            if (fieldDef.itemMaxCnt != TBaseConstants.META_VALUE_UNDEFINED) {
                if (valItemSet.size() > fieldDef.itemMaxCnt) {
                    result.setFailResult(new StringBuilder(512)
                            .append("Parameter ").append(fieldDef.name)
                            .append("'s item count over max allowed count (")
                            .append(fieldDef.itemMaxCnt).append(")!").toString());
                }
            }
            result.setSuccResult(valItemSet);
        } else {
            if (!checkStrValueNorms(fieldDef, paramValue, result)) {
                return result.success;
            }
            result.setSuccResult(paramValue);
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an json dict
     *
     * @param req         Http Servlet Request
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static boolean getJsonDictParamValue(HttpServletRequest req,
                                                WebFieldDef fieldDef,
                                                boolean required,
                                                Map<String, Long> defValue,
                                                ProcessResult result) {
        // get parameter value
        String paramValue = req.getParameter(fieldDef.name);
        if (paramValue == null) {
            paramValue = req.getParameter(fieldDef.shortName);
        }
        if (TStringUtils.isNotBlank(paramValue)) {
            // Cleanup value extra characters
            paramValue = escDoubleQuotes(paramValue.trim());
        }
        // Check if the parameter exists
        if (TStringUtils.isBlank(paramValue)) {
            if (required) {
                result.setFailResult(new StringBuilder(512)
                        .append("Parameter ").append(fieldDef.name)
                        .append(" is missing or value is null or blank!").toString());
            } else {
                result.setSuccResult(defValue);
            }
            return result.success;
        }
        try {
            paramValue = URLDecoder.decode(paramValue,
                    TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parameter ").append(fieldDef.name)
                    .append(" decode error, exception is ")
                    .append(e.toString()).toString());
        }
        if (TStringUtils.isBlank(paramValue)) {
            if (required) {
                result.setFailResult(new StringBuilder(512).append("Parameter ")
                        .append(fieldDef.name).append("'s value is blank!").toString());
            } else {
                result.setSuccResult(defValue);
            }
            return result.success;
        }
        if (fieldDef.valMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (paramValue.length() > fieldDef.valMaxLen) {
                result.setFailResult(new StringBuilder(512)
                        .append("Parameter ").append(fieldDef.name)
                        .append("'s length over max allowed length (")
                        .append(fieldDef.valMaxLen).append(")!").toString());
                return result.success;
            }
        }
        // parse data
        try {
            Map<String, Long> manOffsets = new Gson().fromJson(paramValue,
                    new TypeToken<Map<String, Long>>(){}.getType());
            result.setSuccResult(manOffsets);
        } catch (Throwable e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parameter ").append(fieldDef.name)
                    .append(" value parse failure, error is ")
                    .append(e.getMessage()).append("!").toString());
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an string value to Date value
     *
     * @param req         Http Servlet Request
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if failed to parse value from the given object
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static boolean getDateParameter(HttpServletRequest req,
                                           WebFieldDef fieldDef,
                                           boolean required,
                                           Date defValue,
                                           ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, result)) {
            return result.success;
        }
        String paramValue = (String) result.retData1;
        if (paramValue == null) {
            result.setSuccResult(defValue);
            return result.success;
        }
        try {
            DateFormat sdf = new SimpleDateFormat(TBaseConstants.META_TMP_DATE_VALUE);
            Date date = sdf.parse(paramValue);
            result.setSuccResult(date);
        } catch (Throwable e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
        }
        return result.success;
    }

    /**
     * Valid execution authorization info
     * @param req        Http Servlet Request
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param master     current master object
     * @param result     process result
     * @return valid result for the parameter value
     */
    public static boolean validReqAuthorizeInfo(HttpServletRequest req,
                                                WebFieldDef fieldDef,
                                                boolean required,
                                                TMaster master,
                                                ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, result)) {
            return result.success;
        }
        String paramValue = (String) result.retData1;
        if (paramValue != null) {
            if (!paramValue.equals(master.getMasterConfig().getConfModAuthToken())) {
                result.setFailResult("Illegal access, unauthorized request!");
            }
        }
        return result.success;
    }

    /**
     * process string default value
     *
     * @param isCompFieldType   the parameter if compound field type
     * @param defValue   the parameter default value
     * @param result process result
     * @return process result for default value of parameter
     */
    private static boolean procStringDefValue(boolean isCompFieldType,
                                              String defValue,
                                              ProcessResult result) {
        if (isCompFieldType) {
            Set<String> valItemSet = new HashSet<>();
            if (TStringUtils.isNotBlank(defValue)) {
                valItemSet.add(defValue);
            }
            result.setSuccResult(valItemSet);
        } else {
            result.setSuccResult(defValue);
        }
        return result.success;
    }

    /**
     * Parse the parameter string value by regex define
     *
     * @param fieldDef     the parameter field definition
     * @param paramVal     the parameter value
     * @param result       process result
     * @return check result for string value of parameter
     */
    private static boolean checkStrValueNorms(WebFieldDef fieldDef,
                                              String paramVal,
                                              ProcessResult result) {
        paramVal = paramVal.trim();
        if (TStringUtils.isBlank(paramVal)) {
            result.setSuccResult(null);
            return true;
        }
        // check value's max length
        if (fieldDef.valMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (paramVal.length() > fieldDef.valMaxLen) {
                result.setFailResult(new StringBuilder(512)
                        .append("over max length for ").append(fieldDef.name)
                        .append(", only allow ").append(fieldDef.valMaxLen)
                        .append(" length").toString());
                return false;
            }
        }
        // check value's pattern
        if (fieldDef.regexCheck) {
            if (!paramVal.matches(fieldDef.regexDef.getPattern())) {
                result.setFailResult(new StringBuilder(512)
                        .append("illegal value for ").append(fieldDef.name)
                        .append(", value ").append(fieldDef.regexDef.getErrMsgTemp())
                        .toString());
                return false;
            }
        }
        result.setSuccResult(paramVal);
        return true;
    }

    /**
     * Parse the parameter string value by regex define
     *
     * @param fieldDef     the parameter field definition
     * @param paramValue   the parameter value
     * @param hasMinVal    whether there is a minimum
     * param minValue      the parameter min value
     * @param hasMaxVal    whether there is a maximum
     * param maxValue      the parameter max value
     * @param result   process result
     * @return check result for string value of parameter
     */
    private static boolean checkIntValueNorms(WebFieldDef fieldDef,
                                              String paramValue,
                                              boolean hasMinVal,
                                              int minValue,
                                              boolean hasMaxVal,
                                              int maxValue,
                                              ProcessResult result) {
        try {
            int paramIntVal = Integer.parseInt(paramValue);
            if (hasMinVal && paramIntVal < minValue) {
                result.setFailResult(new StringBuilder(512)
                        .append("Parameter ").append(fieldDef.name)
                        .append(" value must >= ").append(minValue).toString());
                return false;
            }
            if (hasMaxVal && paramIntVal > maxValue) {
                result.setFailResult(new StringBuilder(512)
                        .append("Parameter ").append(fieldDef.name)
                        .append(" value must <= ").append(maxValue).toString());
                return false;
            }
            result.setSuccResult(paramIntVal);
        } catch (Throwable e) {
            result.setFailResult(new StringBuilder(512)
                    .append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
            return false;
        }
        return true;
    }

    /**
     * Parse the parameter value from an object value to ip address of string value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param paramMaxLen  the max length of string to return
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return a ip string of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validAddressParameter(String paramName, String paramValue, int paramMaxLen,
                                               boolean required, String defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        if (tmpParamValue.length() > paramMaxLen) {
            throw new Exception(new StringBuilder(512).append("the max length of ")
                    .append(paramName).append(" parameter is ").append(paramMaxLen)
                    .append(" characters").toString());
        }
        if (!tmpParamValue.matches(TBaseConstants.META_TMP_IP_ADDRESS_VALUE)) {
            throw new Exception(new StringBuilder(512)
                    .append("the value of ").append(paramName)
                    .append(" parameter not matches the regulation :")
                    .append(TBaseConstants.META_TMP_IP_ADDRESS_VALUE).toString());
        }
        return tmpParamValue;
    }

    /**
     * Decode the parameter value from an object value
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param paramMaxLen  the max length of string to return
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return the decoded string of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validDecodeStringParameter(String paramName, String paramValue, int paramMaxLen,
                                                    boolean required, String defaultValue) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        String output = null;
        try {
            output = URLDecoder.decode(tmpParamValue, TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            throw new Exception(new StringBuilder(512).append("Decode ")
                    .append(paramName).append("error, exception is ")
                    .append(e.toString()).toString());
        }
        if (paramMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (output.length() > paramMaxLen) {
                throw new Exception(new StringBuilder(512)
                        .append("the max length of ").append(paramName)
                        .append(" parameter is ").append(paramMaxLen)
                        .append(" characters").toString());
            }
        }
        return output;
    }

    /**
     * Proceed authorization
     *
     * @param master           the service master
     * @param brokerConfManager the broker configuration manager
     * @param reqToken         the token for checking
     * @throws Exception if authorization failed
     */
    public static void reqAuthorizeCheck(TMaster master,
                                          BrokerConfManager brokerConfManager,
                                          String reqToken) throws Exception {
        if (brokerConfManager.isPrimaryNodeActive()) {
            throw new Exception(
                    "Illegal visit: designatedPrimary happened...please check if the other member is down");
        }
        String inPutConfModAuthToken =
                validStringParameter("confModAuthToken", reqToken,
                        TServerConstants.CFG_MODAUTHTOKEN_MAX_LENGTH, true, "");
        if (!inPutConfModAuthToken.equals(master.getMasterConfig().getConfModAuthToken())) {
            throw new Exception("Illegal visit: not authorized to process authorization info!");
        }
    }

    /**
     * Decode the deletePolicy parameter value from an object value
     * the value must like {method},{digital}[s|m|h]
     *
     * @param paramName    the parameter name
     * @param paramValue   the parameter value which is an object for parsing
     * @param required     a boolean value represent whether the parameter is must required
     * @param defaultValue a default value returned if failed to parse value from the given object
     * @return the decoded string of parameter
     * @throws Exception if failed to parse the object
     */
    public static String validDeletePolicyParameter(String paramName, String paramValue,
                                                    boolean required, String defaultValue) throws Exception {
        int paramMaxLen = TServerConstants.CFG_DELETEPOLICY_MAX_LENGTH;
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue)) {
            return defaultValue;
        }
        String inDelPolicy = null;
        try {
            inDelPolicy = URLDecoder.decode(tmpParamValue, TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            throw new Exception(new StringBuilder(512).append("Decode ")
                    .append(paramName).append("error, exception is ")
                    .append(e.toString()).toString());
        }
        if (inDelPolicy.length() > paramMaxLen) {
            throw new Exception(new StringBuilder(512)
                    .append("the max length of ").append(paramName)
                    .append(" parameter is ").append(paramMaxLen)
                    .append(" characters").toString());
        }
        String[] tmpStrs = inDelPolicy.split(",");
        if (tmpStrs.length != 2) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: must include one and only one comma character,")
                    .append(" the format of ").append(paramName)
                    .append(" must like {method},{digital}[m|s|h]").toString());
        }
        if (TStringUtils.isBlank(tmpStrs[0])) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: method's value must not be blank!")
                    .append(" the format of ").append(paramName)
                    .append(" must like {method},{digital}[s|m|h]").toString());
        }
        if (!"delete".equalsIgnoreCase(tmpStrs[0].trim())) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: only support delete method now!").toString());
        }
        String validValStr = tmpStrs[1];
        String timeUnit = validValStr.substring(validValStr.length() - 1).toLowerCase();
        if (Character.isLetter(timeUnit.charAt(0))) {
            if (!allowedDelUnits.contains(timeUnit)) {
                throw new Exception(new StringBuilder(512)
                        .append("Illegal value: only support [s|m|h] unit!").toString());
            }
        }
        long validDuration = 0;
        try {
            if (timeUnit.endsWith("s")) {
                validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 1000;
            } else if (timeUnit.endsWith("m")) {
                validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 60000;
            } else if (timeUnit.endsWith("h")) {
                validDuration = Long.parseLong(validValStr.substring(0, validValStr.length() - 1)) * 3600000;
            } else {
                validDuration = Long.parseLong(validValStr) * 3600000;
            }
        } catch (Throwable e) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: the value of valid duration must digits!").toString());
        }
        if (validDuration <= 0 || validDuration > DataStoreUtils.MAX_FILE_VALID_DURATION) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: the value of valid duration must")
                    .append(" be greater than 0 and  less than or equal to ")
                    .append(DataStoreUtils.MAX_FILE_VALID_DURATION).append(" seconds!").toString());
        }
        if (Character.isLetter(timeUnit.charAt(0))) {
            return new StringBuilder(512).append("delete,")
                    .append(validValStr.substring(0, validValStr.length() - 1))
                    .append(timeUnit).toString();
        } else {
            return new StringBuilder(512).append("delete,")
                    .append(validValStr).append("h").toString();
        }
    }

    /**
     * check the filter conditions and get them
     *
     * @param inFilterConds the filter conditions to be decoded
     * @param isTransBlank  denote whether it translate blank condition
     * @param sb            the string buffer used to construct result
     * @return the decoded filter conditions
     * @throws Exception if failed to decode the filter conditions
     */
    public static String checkAndGetFilterConds(String inFilterConds,
                                                boolean isTransBlank,
                                                StringBuilder sb) throws Exception {
        if (TStringUtils.isNotBlank(inFilterConds)) {
            inFilterConds = escDoubleQuotes(inFilterConds.trim());
        }
        if (TStringUtils.isBlank(inFilterConds)) {
            if (isTransBlank) {
                sb.append(TServerConstants.TOKEN_BLANK_FILTER_CONDITION);
            }
        } else {
            sb.append(TokenConstants.ARRAY_SEP);
            TreeSet<String> filterConds = new TreeSet<>();
            String[] strFilterConds = inFilterConds.split(TokenConstants.ARRAY_SEP);
            for (int i = 0; i < strFilterConds.length; i++) {
                if (TStringUtils.isBlank(strFilterConds[i])) {
                    continue;
                }
                String filterCond = strFilterConds[i].trim();
                if (filterCond.length() > TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_LENGTH) {
                    sb.delete(0, sb.length());
                    throw new Exception(sb.append("Illegal value: the max length of ")
                            .append(filterCond).append(" in filterConds parameter over ")
                            .append(TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_LENGTH)
                            .append(" characters").toString());
                }
                if (!filterCond.matches(TBaseConstants.META_TMP_FILTER_VALUE)) {
                    sb.delete(0, sb.length());
                    throw new Exception(sb.append("Illegal value: the value of ")
                        .append(filterCond).append(" in filterCond parameter ")
                        .append("must only contain characters,numbers,and underscores").toString());
                }
                filterConds.add(filterCond);
            }
            int count = 0;
            for (String itemStr : filterConds) {
                if (count++ > 0) {
                    sb.append(TokenConstants.ARRAY_SEP);
                }
                sb.append(itemStr);
            }
            sb.append(TokenConstants.ARRAY_SEP);
        }
        String strNewFilterConds = sb.toString();
        sb.delete(0, sb.length());
        return strNewFilterConds;
    }

    /**
     * check the filter conditions and get them in a set
     *
     * @param inFilterConds the filter conditions to be decoded
     * @param transCondItem whether to translate condition item
     * @param checkTotalCnt whether to check condition item exceed max count
     * @param sb            the string buffer used to construct result
     * @return the decoded filter conditions
     * @throws Exception if failed to decode the filter conditions
     */
    public static Set<String> checkAndGetFilterCondSet(String inFilterConds,
                                                       boolean transCondItem,
                                                       boolean checkTotalCnt,
                                                       StringBuilder sb) throws Exception {
        Set<String> filterCondSet = new HashSet<>();
        if (TStringUtils.isBlank(inFilterConds)) {
            return filterCondSet;
        }
        inFilterConds = escDoubleQuotes(inFilterConds.trim());
        if (TStringUtils.isNotBlank(inFilterConds)) {
            String[] strFilterConds = inFilterConds.split(TokenConstants.ARRAY_SEP);
            for (int i = 0; i < strFilterConds.length; i++) {
                if (TStringUtils.isBlank(strFilterConds[i])) {
                    continue;
                }
                String filterCond = strFilterConds[i].trim();
                if (filterCond.length() > TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT) {
                    sb.delete(0, sb.length());
                    throw new Exception(sb.append("Illegal value: the max length of ")
                            .append(filterCond).append(" in filterConds parameter over ")
                            .append(TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT)
                            .append(" characters").toString());
                }
                if (!filterCond.matches(TBaseConstants.META_TMP_FILTER_VALUE)) {
                    sb.delete(0, sb.length());
                    throw new Exception(sb.append("Illegal value: the value of ")
                        .append(filterCond).append(" in filterCond parameter must ")
                        .append("only contain characters,numbers,and underscores").toString());
                }
                if (transCondItem) {
                    filterCondSet.add(sb.append(TokenConstants.ARRAY_SEP)
                            .append(filterCond).append(TokenConstants.ARRAY_SEP).toString());
                    sb.delete(0, sb.length());
                } else {
                    filterCondSet.add(filterCond);
                }
            }
            if (checkTotalCnt) {
                if (filterCondSet.size() > TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT) {
                    throw new Exception(sb.append("Illegal value: the count of filterCond's ")
                        .append("value over max allowed count(")
                        .append(TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT).append(")!").toString());
                }
            }
        }
        return filterCondSet;
    }

    public static Set<String> getBatchGroupNames(String inputGroupName,
                                                 boolean checkEmpty,
                                                 boolean checkResToken,
                                                 Set<String> resTokens,
                                                 final StringBuilder sb) throws Exception {
        Set<String> batchOpGroupNames = new HashSet<>();
        if (TStringUtils.isNotBlank(inputGroupName)) {
            inputGroupName = escDoubleQuotes(inputGroupName.trim());
        }
        if (TStringUtils.isBlank(inputGroupName)) {
            if (checkEmpty) {
                throw new Exception("Illegal value: required groupName parameter");
            }
            return batchOpGroupNames;
        }
        String[] strGroupNames = inputGroupName.split(TokenConstants.ARRAY_SEP);
        if (strGroupNames.length > TServerConstants.CFG_BATCH_RECORD_OPERATE_MAX_COUNT) {
            throw new Exception(sb.append("Illegal value: groupName's batch count over max count ")
                .append(TServerConstants.CFG_BATCH_RECORD_OPERATE_MAX_COUNT).toString());
        }
        for (int i = 0; i < strGroupNames.length; i++) {
            if (TStringUtils.isBlank(strGroupNames[i])) {
                continue;
            }
            String groupName = strGroupNames[i].trim();
            if (checkResToken) {
                if (resTokens != null && !resTokens.isEmpty()) {
                    if (resTokens.contains(groupName)) {
                        throw new Exception(sb.append("Illegal value: in groupName parameter, '")
                            .append(groupName).append("' is a system reserved token!").toString());
                    }
                }
            }
            if (groupName.length() > TBaseConstants.META_MAX_GROUPNAME_LENGTH) {
                throw new Exception(sb.append("Illegal value: the max length of ")
                        .append(groupName).append(" in groupName parameter over ")
                        .append(TBaseConstants.META_MAX_GROUPNAME_LENGTH)
                        .append(" characters").toString());
            }
            if (!groupName.matches(TBaseConstants.META_TMP_GROUP_VALUE)) {
                throw new Exception(sb.append("Illegal value: the value of ").append(groupName)
                    .append("in groupName parameter must begin with a letter, can only contain ")
                    .append("characters,numbers,hyphen,and underscores").toString());
            }
            batchOpGroupNames.add(groupName);
        }
        if (batchOpGroupNames.isEmpty()) {
            if (checkEmpty) {
                throw new Exception("Illegal value: Null value of groupName parameter");
            }
        }
        return batchOpGroupNames;
    }

    public static Set<String> getBatchTopicNames(String inputTopicName,
                                                 boolean checkEmpty,
                                                 boolean checkRange,
                                                 Set<String> checkedTopicList,
                                                 final StringBuilder sb) throws Exception {
        Set<String> batchOpTopicNames = new HashSet<>();
        if (TStringUtils.isNotBlank(inputTopicName)) {
            inputTopicName = escDoubleQuotes(inputTopicName.trim());
        }
        if (TStringUtils.isBlank(inputTopicName)) {
            if (checkEmpty) {
                throw new Exception("Illegal value: required topicName parameter");
            }
            return batchOpTopicNames;
        }
        String[] strTopicNames = inputTopicName.split(TokenConstants.ARRAY_SEP);
        if (strTopicNames.length > TServerConstants.CFG_BATCH_RECORD_OPERATE_MAX_COUNT) {
            throw new Exception(sb.append("Illegal value: topicName's batch count over max count ")
                    .append(TServerConstants.CFG_BATCH_RECORD_OPERATE_MAX_COUNT).toString());
        }
        for (int i = 0; i < strTopicNames.length; i++) {
            if (TStringUtils.isBlank(strTopicNames[i])) {
                continue;
            }
            String topicName = strTopicNames[i].trim();
            if (topicName.length() > TBaseConstants.META_MAX_TOPICNAME_LENGTH) {
                throw new Exception(sb.append("Illegal value: the max length of ")
                        .append(topicName).append(" in topicName parameter over ")
                        .append(TBaseConstants.META_MAX_TOPICNAME_LENGTH)
                        .append(" characters").toString());
            }
            if (!topicName.matches(TBaseConstants.META_TMP_STRING_VALUE)) {
                throw new Exception(sb.append("Illegal value: the value of ")
                    .append(topicName).append(" in topicName parameter must begin with a letter,")
                    .append(" can only contain characters,numbers,and underscores").toString());
            }
            if (checkRange) {
                if (!checkedTopicList.contains(topicName)) {
                    throw new Exception(sb.append("Illegal value: topic(").append(topicName)
                            .append(") not configure in master's topic configure, please configure first!").toString());
                }
            }
            batchOpTopicNames.add(topicName);
        }
        if (batchOpTopicNames.isEmpty()) {
            if (checkEmpty) {
                throw new Exception("Illegal value: Null value of topicName parameter");
            }
        }
        return batchOpTopicNames;
    }

    public static Set<String> getBatchBrokerIpSet(String inStrBrokerIps,
                                                  boolean checkEmpty) throws Exception {
        Set<String> batchBrokerIps = new HashSet<>();
        if (TStringUtils.isNotBlank(inStrBrokerIps)) {
            inStrBrokerIps = escDoubleQuotes(inStrBrokerIps.trim());
        }
        if (TStringUtils.isBlank(inStrBrokerIps)) {
            if (checkEmpty) {
                throw new Exception("Illegal value: required brokerIp parameter");
            }
            return batchBrokerIps;
        }
        String[] strBrokerIps = inStrBrokerIps.split(TokenConstants.ARRAY_SEP);
        for (int i = 0; i < strBrokerIps.length; i++) {
            if (TStringUtils.isEmpty(strBrokerIps[i])) {
                continue;
            }
            String brokerIp =
                    checkParamCommonRequires("brokerIp", strBrokerIps[i], true);
            if (batchBrokerIps.contains(brokerIp)) {
                continue;
            }
            batchBrokerIps.add(brokerIp);
        }
        if (batchBrokerIps.isEmpty()) {
            if (checkEmpty) {
                throw new Exception("Illegal value: Null value of brokerIp parameter");
            }
        }
        return batchBrokerIps;
    }

    public static Set<Integer> getBatchBrokerIdSet(String inStrBrokerIds,
                                                   boolean checkEmpty) throws Exception {
        Set<Integer> batchBrokerIdSet = new HashSet<>();
        if (TStringUtils.isNotBlank(inStrBrokerIds)) {
            inStrBrokerIds = escDoubleQuotes(inStrBrokerIds.trim());
        }
        if (TStringUtils.isBlank(inStrBrokerIds)) {
            if (checkEmpty) {
                throw new Exception("Illegal value: required brokerId parameter");
            }
            return batchBrokerIdSet;
        }
        String[] strBrokerIds = inStrBrokerIds.split(TokenConstants.ARRAY_SEP);
        if (strBrokerIds.length > TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT) {
            throw new Exception(new StringBuilder(512)
                    .append("Illegal value: batch numbers of brokerId's value over max count ")
                    .append(TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT).toString());
        }
        for (int i = 0; i < strBrokerIds.length; i++) {
            if (TStringUtils.isEmpty(strBrokerIds[i])) {
                continue;
            }
            int brokerId =
                    validIntDataParameter("brokerId", strBrokerIds[i], true, 0, 1);
            batchBrokerIdSet.add(brokerId);
        }
        if (batchBrokerIdSet.isEmpty()) {
            if (checkEmpty) {
                throw new Exception("Illegal value: Null value of brokerId parameter");
            }
        }
        return batchBrokerIdSet;
    }

    public static Set<BdbBrokerConfEntity> getBatchBrokerIdSet(String inStrBrokerIds,
                                                               BrokerConfManager webMaster,
                                                               boolean checkEmpty,
                                                               final StringBuilder sb) throws Exception {
        Set<BdbBrokerConfEntity> batchBrokerIdSet = new HashSet<>();
        if (TStringUtils.isNotBlank(inStrBrokerIds)) {
            inStrBrokerIds = escDoubleQuotes(inStrBrokerIds.trim());
        }
        if (TStringUtils.isBlank(inStrBrokerIds)) {
            if (checkEmpty) {
                throw new Exception("Illegal value: required brokerId parameter");
            }
            return batchBrokerIdSet;
        }
        String[] strBrokerIds = inStrBrokerIds.split(TokenConstants.ARRAY_SEP);
        if (strBrokerIds.length > TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT) {
            throw new Exception(sb
                    .append("Illegal value: batch numbers of brokerId's value over max count ")
                    .append(TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT).toString());
        }
        for (int i = 0; i < strBrokerIds.length; i++) {
            if (TStringUtils.isEmpty(strBrokerIds[i])) {
                continue;
            }
            int brokerId =
                    validIntDataParameter("brokerId", strBrokerIds[i], true, 0, 1);
            BdbBrokerConfEntity brokerConfEntity =
                    webMaster.getBrokerDefaultConfigStoreInfo(brokerId);
            if (brokerConfEntity == null) {
                throw new Exception(sb
                        .append("Illegal value: not found broker default configure record by brokerId=")
                        .append(brokerId).toString());
            }
            batchBrokerIdSet.add(brokerConfEntity);
        }
        if (batchBrokerIdSet.isEmpty()) {
            if (checkEmpty) {
                throw new Exception("Illegal value: Null value of brokerId parameter");
            }
        }
        return batchBrokerIdSet;
    }

    /**
     * check and get parameter value with json array
     *
     * @param paramName   the parameter name
     * @param paramValue  the object value of the parameter
     * @param paramMaxLen the maximum length of json array
     * @param required    denote whether the parameter is must required
     * @return a list of linked hash map represent the json array
     * @throws Exception
     */
    public static List<Map<String, String>> checkAndGetJsonArray(String paramName,
                                                                 String paramValue,
                                                                 int paramMaxLen,
                                                                 boolean required) throws Exception {
        String tmpParamValue = checkParamCommonRequires(paramName, paramValue, required);
        if (TStringUtils.isBlank(tmpParamValue) && !required) {
            return null;
        }
        String decTmpParamVal = null;
        try {
            decTmpParamVal = URLDecoder.decode(tmpParamValue,
                    TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            throw new Exception(new StringBuilder(512).append("Decode ")
                    .append(paramName).append("error, exception is ")
                    .append(e.toString()).toString());
        }
        if (TStringUtils.isBlank(decTmpParamVal)) {
            if (required) {
                throw new Exception(new StringBuilder(512)
                        .append("Blank value of ").append(paramName)
                        .append(" parameter").toString());
            } else {
                return null;
            }
        }
        if (paramMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (decTmpParamVal.length() > paramMaxLen) {
                throw new Exception(new StringBuilder(512)
                        .append("the max length of ").append(paramName)
                        .append(" parameter is ").append(paramMaxLen)
                        .append(" characters").toString());
            }
        }
        return new Gson().fromJson(decTmpParamVal, new TypeToken<List<Map<String, String>>>(){}.getType());
    }

    /**
     * Check the broker online status
     *
     * @param curEntity the entity of bdb broker configuration
     * @return the true if broker is online, false in other cases
     */
    public static boolean checkBrokerInOnlineStatus(BdbBrokerConfEntity curEntity) {
        if (curEntity != null) {
            return (curEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE
                    || curEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE
                    || curEntity.getManageStatus() == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ);
        }
        return false;
    }

    /**
     * check whether the broker is working in progress
     *
     * @param brokerId  the id of the broker
     * @param webMaster the broker configuration manager
     * @param sBuilder  the string builder used to construct the detail err
     * @return true if the broker is working in progress, false in other cases
     * @throws Exception
     */
    public static boolean checkBrokerInProcessing(int brokerId,
                                                  BrokerConfManager webMaster,
                                                  StringBuilder sBuilder) throws Exception {
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                webMaster.getBrokerRunSyncStatusInfo(brokerId);
        if ((brokerSyncStatusInfo != null) && (brokerSyncStatusInfo.isBrokerRegister())) {
            int status = brokerSyncStatusInfo.getBrokerRunStatus();
            if (!((status == TStatusConstants.STATUS_SERVICE_UNDEFINED)
                    || (status == TStatusConstants.STATUS_SERVICE_TOONLINE_WAIT_REGISTER)
                    || (status == TStatusConstants.STATUS_SERVICE_TOONLINE_PART_WAIT_REGISTER))) {
                if (sBuilder != null) {
                    sBuilder.append("Illegal value: the broker of brokerId=")
                            .append(brokerId).append(" is processing event(")
                            .append(brokerSyncStatusInfo.getBrokerRunStatus())
                            .append("), please try later! ");
                }
                return true;
            }
        }
        return false;
    }

    public static boolean checkBrokerUnLoad(int brokerId,
                                            BrokerConfManager webMaster,
                                            StringBuilder sBuilder) throws Exception {
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                webMaster.getBrokerRunSyncStatusInfo(brokerId);
        if ((brokerSyncStatusInfo != null) && (brokerSyncStatusInfo.isBrokerRegister())) {
            int status = brokerSyncStatusInfo.getBrokerManageStatus();
            if ((status == TStatusConstants.STATUS_MANAGE_ONLINE)
                    || (status == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE)
                    || (status == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ)) {
                if (!brokerSyncStatusInfo.isBrokerLoaded()) {
                    if (sBuilder != null) {
                        sBuilder.append("The broker's configure of brokerId=").append(brokerId)
                                .append(" changed but not reload in online status, please reload configure first!");
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean checkBrokerInOfflining(int brokerId,
                                                 int manageStatus,
                                                 BrokerConfManager webMaster,
                                                 StringBuilder sBuilder) throws Exception {
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                webMaster.getBrokerRunSyncStatusInfo(brokerId);
        if ((brokerSyncStatusInfo != null)
                && (brokerSyncStatusInfo.isBrokerRegister())) {
            if ((manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
                    && (brokerSyncStatusInfo.getBrokerRunStatus() != TStatusConstants.STATUS_SERVICE_UNDEFINED)) {
                if (sBuilder != null) {
                    sBuilder.append("Illegal value: the broker is processing offline event by brokerId=")
                            .append(brokerId).append(", please wait and try later!");
                }
                return true;
            }
        }
        return false;
    }

    public static String getBrokerManageStatusStr(int manageStatus) {
        String strManageStatus = "unsupported_status";
        if (manageStatus == TStatusConstants.STATUS_MANAGE_APPLY) {
            strManageStatus = "draft";
        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE) {
            strManageStatus = "online";
        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE) {
            strManageStatus = "offline";
        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
            strManageStatus = "only-read";
        } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
            strManageStatus = "only-write";
        }
        return strManageStatus;
    }

    public static String date2yyyyMMddHHmmss(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(date);
    }

    public static String checkParamCommonRequires(final String paramName, final String paramValue,
                                                   boolean required) throws Exception {
        String temParamValue = null;
        if (paramValue == null) {
            if (required) {
                throw new Exception(new StringBuilder(512).append("Required ")
                        .append(paramName).append(" parameter").toString());
            }
        } else {
            temParamValue = escDoubleQuotes(paramValue.trim());
            if (TStringUtils.isBlank(temParamValue)) {
                if (required) {
                    throw new Exception(new StringBuilder(512)
                            .append("Null or blank value of ").append(paramName)
                            .append(" parameter").toString());
                }
            }
        }
        return temParamValue;
    }

    private static String escDoubleQuotes(String inPutStr) {
        if (TStringUtils.isBlank(inPutStr) || inPutStr.length() < 2) {
            return inPutStr;
        }
        if (inPutStr.charAt(0) == '\"'
                && inPutStr.charAt(inPutStr.length() - 1) == '\"') {
            if (inPutStr.length() == 2) {
                return "";
            } else {
                return inPutStr.substring(1, inPutStr.length() - 1).trim();
            }
        }
        return inPutStr;
    }
}
