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
import java.util.ArrayList;
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
import org.apache.tubemq.corebase.policies.FlowCtrlItem;
import org.apache.tubemq.corebase.policies.FlowCtrlRuleHandler;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.apache.tubemq.corebase.utils.Tuple2;
import org.apache.tubemq.server.broker.utils.DataStoreUtils;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.TStatusConstants;
import org.apache.tubemq.server.common.fielddef.WebFieldDef;
import org.apache.tubemq.server.common.statusdef.TopicStatus;
import org.apache.tubemq.server.master.TMaster;
import org.apache.tubemq.server.master.bdbstore.bdbentitys.BdbBrokerConfEntity;
import org.apache.tubemq.server.master.metamanage.DataOpErrCode;
import org.apache.tubemq.server.master.metamanage.MetaDataManager;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerConfManager;
import org.apache.tubemq.server.master.nodemanage.nodebroker.BrokerSyncStatusInfo;


public class WebParameterUtils {

    private static final List<String> allowedDelUnits = Arrays.asList("s", "m", "h");
    private static final List<Integer> allowedPriorityVal = Arrays.asList(1, 2, 3);


    public static StringBuilder buildFailResult(StringBuilder strBuffer, String errMsg) {
        return strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"")
                .append(errMsg).append("\"}");
    }

    public static StringBuilder buildFailResultWithBlankData(String errMsg,
                                                             StringBuilder strBuffer) {
        return buildFailResultWithBlankData(400, errMsg, strBuffer);
    }

    public static StringBuilder buildFailResultWithBlankData(int errcode, String errMsg,
                                                             StringBuilder strBuffer) {
        return strBuffer.append("{\"result\":false,\"errCode\":").append(errcode)
                .append(",\"errMsg\":\"").append(errMsg).append("\",\"data\":[]}");
    }

    public static StringBuilder buildSuccessResult(StringBuilder strBuffer) {
        return strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\"}");
    }

    public static StringBuilder buildSuccessResult(StringBuilder strBuffer, String appendInfo) {
        return strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"").
                append(appendInfo).append("\"}");
    }

    public static StringBuilder buildSuccessWithDataRetBegin(StringBuilder strBuffer) {
        return strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"OK\",\"data\":[");
    }

    public static StringBuilder buildSuccessWithDataRetEnd(
            StringBuilder strBuffer, int totalCnt) {
        return strBuffer.append("],\"count\":").append(totalCnt).append("}");
    }

    public static StringBuilder buildSuccWithData(long dataVerId,
                                                  StringBuilder strBuffer) {
        List<Long> dataVerIds = new ArrayList<>(1);
        dataVerIds.add(dataVerId);
        return buildSuccWithData("Ok", dataVerIds, strBuffer);
    }

    public static StringBuilder buildSuccWithData(String errMsg,
                                                  List<Long> dataVerIds,
                                                  StringBuilder strBuffer) {
        int count = 0;
        strBuffer.append("{\"result\":true,\"errCode\":0,\"errMsg\":\"")
                .append(errMsg).append("\",\"data\":[");
        for (Long dataVerId : dataVerIds) {
            if (dataVerId == null) {
                continue;
            }
            if (count++ > 0) {
                strBuffer.append(",");
            }
            strBuffer.append("{\"").append(WebFieldDef.DATAVERSIONID.name)
                    .append("\":").append(dataVerId).append("}");
        }
        strBuffer.append("],\"count\":").append(count).append("}");
        return strBuffer;
    }

    public static <T> boolean getAUDBaseInfo(T paramCntr, boolean isAdd,
                                             BaseEntity defOpEntity,
                                             StringBuilder sBuffer,
                                             ProcessResult result) {
        // check and get data version id
        if (!WebParameterUtils.getLongParamValue(paramCntr, WebFieldDef.DATAVERSIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED, sBuffer, result)) {
            return result.isSuccess();
        }
        long dataVerId = (long) result.retData1;
        // check and get createUser or modifyUser
        String createUsr = "";
        Date createDate = null;
        if (isAdd) {
            // check create user field
            if (!WebParameterUtils.getStringParamValue(paramCntr, WebFieldDef.CREATEUSER,
                    (defOpEntity == null && isAdd),
                    (defOpEntity == null ? null : defOpEntity.getCreateUser()),
                    sBuffer, result)) {
                return result.isSuccess();
            }
            createUsr = (String) result.retData1;
            // check and get create date
            if (!WebParameterUtils.getDateParameter(paramCntr, WebFieldDef.CREATEDATE, false,
                    (defOpEntity == null ? new Date() : defOpEntity.getCreateDate()),
                    sBuffer, result)) {
                return result.isSuccess();
            }
            createDate = (Date) result.retData1;
        }
        // check modify user field
        if (!WebParameterUtils.getStringParamValue(paramCntr, WebFieldDef.MODIFYUSER,
                (defOpEntity == null && !isAdd),
                (defOpEntity == null ? createUsr : defOpEntity.getModifyUser()),
                sBuffer, result)) {
            return result.isSuccess();
        }
        String modifyUser = (String) result.retData1;
        // check and get modify date
        if (!WebParameterUtils.getDateParameter(paramCntr, WebFieldDef.MODIFYDATE, false,
                (defOpEntity == null ? createDate : defOpEntity.getModifyDate()),
                sBuffer, result)) {
            return result.isSuccess();
        }
        Date modifyDate = (Date) result.retData1;
        result.setSuccResult(new BaseEntity(dataVerId,
                createUsr, createDate, modifyUser, modifyDate));
        return result.isSuccess();
    }

    /**
     * Parse the parameter value required for query
     *
     * @param req        Http Servlet Request
     * @param qryEntity  query entity
     * @param result     process result
     * @return process result
     */
    public static boolean getQueriedOperateInfo(HttpServletRequest req, BaseEntity qryEntity,
                                                StringBuilder sBuffer, ProcessResult result) {
        // check and get data version id
        if (!WebParameterUtils.getLongParamValue(req, WebFieldDef.DATAVERSIONID,
                false, TBaseConstants.META_VALUE_UNDEFINED, sBuffer, result)) {
            return result.isSuccess();
        }
        long dataVerId = (long) result.retData1;
        // check createUser user field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.CREATEUSER, false, null, sBuffer, result)) {
            return result.isSuccess();
        }
        String createUser = (String) result.retData1;
        // check modify user field
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.MODIFYUSER, false, null, sBuffer, result)) {
            return result.isSuccess();
        }
        String modifyUser = (String) result.retData1;
        // set query keys
        qryEntity.updBaseModifyInfo(dataVerId,
                createUser, null, modifyUser, null, null);
        result.setSuccResult(qryEntity);
        return result.isSuccess();
    }

    public static <T> boolean getQryPriorityIdParameter(T paramCntr, boolean required,
                                                        int defValue, int minValue,
                                                        StringBuilder sBuffer,
                                                        ProcessResult result) {
        if (!getIntParamValue(paramCntr, WebFieldDef.QRYPRIORITYID,
                required, defValue, minValue, sBuffer, result)) {
            return result.success;
        }
        int qryPriorityId = (int) result.retData1;
        if (qryPriorityId > 303 || qryPriorityId < 101) {
            result.setFailResult(sBuffer.append("Illegal value in ")
                    .append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" parameter: ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" value must be greater than or equal")
                    .append(" to 101 and less than or equal to 303!").toString());
            sBuffer.delete(0, sBuffer.length());
            return false;
        }
        if (!allowedPriorityVal.contains(qryPriorityId % 100)) {
            result.setFailResult(sBuffer.append("Illegal value in ")
                    .append(WebFieldDef.QRYPRIORITYID.name).append(" parameter: the units of ")
                    .append(WebFieldDef.QRYPRIORITYID.name).append(" must in ")
                    .append(allowedPriorityVal).toString());
            sBuffer.delete(0, sBuffer.length());
            return false;
        }
        if (!allowedPriorityVal.contains(qryPriorityId / 100)) {
            result.setFailResult(sBuffer.append("Illegal value in ")
                    .append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" parameter: the hundreds of ").append(WebFieldDef.QRYPRIORITYID.name)
                    .append(" must in ").append(allowedPriorityVal).toString());
            sBuffer.delete(0, sBuffer.length());
            return false;
        }
        result.setSuccResult(qryPriorityId);
        return result.isSuccess();
    }

    /**
     * Decode the deletePolicy parameter value from an object value
     * the value must like {method},{digital}[s|m|h]
     *
     * @param paramCntr   parameter container object
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if failed to parse value from the given object
     * @param result     process result of parameter value
     * @return the process result
     */
    public static <T> boolean getDeletePolicyParameter(T paramCntr, boolean required,
                                                       String defValue, StringBuilder sBuffer,
                                                       ProcessResult result) {
        if (!WebParameterUtils.getStringParamValue(paramCntr,
                WebFieldDef.DELETEPOLICY, required, defValue, sBuffer, result)) {
            return result.isSuccess();
        }
        String delPolicy = (String) result.retData1;
        if (TStringUtils.isBlank(delPolicy)) {
            return result.isSuccess();
        }
        // check value format
        String[] tmpStrs = delPolicy.split(",");
        if (tmpStrs.length != 2) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("Value must include one and only one comma character,")
                            .append(" the format of ").append(WebFieldDef.DELETEPOLICY.name())
                            .append(" must like {method},{digital}[m|s|h]").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        if (TStringUtils.isBlank(tmpStrs[0])) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("Method value must not be blank, the format of ")
                            .append(WebFieldDef.DELETEPOLICY.name())
                            .append(" must like {method},{digital}[m|s|h]").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        if (!"delete".equalsIgnoreCase(tmpStrs[0].trim())) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("Field ").append(WebFieldDef.DELETEPOLICY.name())
                            .append(" only support delete method now!").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        String validValStr = tmpStrs[1];
        String timeUnit = validValStr.substring(validValStr.length() - 1).toLowerCase();
        if (Character.isLetter(timeUnit.charAt(0))) {
            if (!allowedDelUnits.contains(timeUnit)) {
                result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                        sBuffer.append("Field ").append(WebFieldDef.DELETEPOLICY.name())
                                .append(" only support [s|m|h] unit!").toString());
                sBuffer.delete(0, sBuffer.length());
                return result.isSuccess();
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
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("The value of field ")
                            .append(WebFieldDef.DELETEPOLICY.name())
                            .append("'s valid duration must digits!").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        if (validDuration <= 0 || validDuration > DataStoreUtils.MAX_FILE_VALID_DURATION) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("The value of field ")
                            .append(WebFieldDef.DELETEPOLICY.name())
                            .append(" must be greater than 0 and  less than or equal to")
                            .append(DataStoreUtils.MAX_FILE_VALID_DURATION)
                            .append(" seconds!").toString());
            sBuffer.delete(0, sBuffer.length());
            return result.isSuccess();
        }
        if (Character.isLetter(timeUnit.charAt(0))) {
            result.setSuccResult(sBuffer.append("delete,")
                    .append(validValStr.substring(0, validValStr.length() - 1))
                    .append(timeUnit).toString());
        } else {
            result.setSuccResult(sBuffer.append("delete,")
                    .append(validValStr).append("h").toString());
        }
        sBuffer.delete(0, sBuffer.length());
        return result.isSuccess();
    }


    public static boolean getTopicStatusParamValue(HttpServletRequest req,
                                                   boolean isRequired,
                                                   TopicStatus defVal,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        // get topicStatusId field
        if (!WebParameterUtils.getIntParamValue(req, WebFieldDef.TOPICSTATUSID,
                isRequired, defVal.getCode(), TopicStatus.STATUS_TOPIC_OK.getCode(),
                sBuffer, result)) {
            return result.isSuccess();
        }
        int paramValue = (int) result.getRetData();
        try {
            TopicStatus topicStatus = TopicStatus.valueOf(paramValue);
            result.setSuccResult(topicStatus);
        } catch (Throwable e) {
            result.setFailResult(DataOpErrCode.DERR_ILLEGAL_VALUE.getCode(),
                    sBuffer.append("The value of field ")
                            .append(WebFieldDef.TOPICSTATUSID.name())
                            .append(" invalid:").append(e.getMessage()).toString());
            sBuffer.delete(0, sBuffer.length());
        }
        return result.isSuccess();
    }

    /**
     * Parse the parameter value for TopicPropGroup class
     *
     * @param paramCntr   parameter container object
     * @param defVal     default value
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getTopicPropInfo(T paramCntr, TopicPropGroup defVal,
                                               StringBuilder sBuffer, ProcessResult result) {
        TopicPropGroup newConf = new TopicPropGroup();
        // get numTopicStores parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.NUMTOPICSTORES, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getNumTopicStores()),
                TServerConstants.TOPIC_STOREBLOCK_NUM_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setNumTopicStores((int) result.retData1);
        // get numPartitions parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.NUMPARTITIONS, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getNumPartitions()),
                TServerConstants.TOPIC_PARTITION_NUM_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setNumPartitions((int) result.retData1);
        // get unflushThreshold parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.UNFLUSHTHRESHOLD, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getUnflushThreshold()),
                TServerConstants.TOPIC_DSK_UNFLUSHTHRESHOLD_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setUnflushThreshold((int) result.retData1);
        // get unflushInterval parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.UNFLUSHINTERVAL, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getUnflushInterval()),
                TServerConstants.TOPIC_DSK_UNFLUSHINTERVAL_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setUnflushInterval((int) result.retData1);
        // get unflushDataHold parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.UNFLUSHINTERVAL, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getUnflushDataHold()),
                TServerConstants.TOPIC_DSK_UNFLUSHDATAHOLD_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setUnflushDataHold((int) result.retData1);
        // get memCacheMsgSizeInMB parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.MCACHESIZEINMB, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getMemCacheMsgSizeInMB()),
                TServerConstants.TOPIC_CACHESIZE_MB_MIN,
                TServerConstants.TOPIC_CACHESIZE_MB_MAX, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setMemCacheMsgSizeInMB((int) result.retData1);
        // get memCacheFlushIntvl parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.UNFMCACHEINTERVAL, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getMemCacheFlushIntvl()),
                TServerConstants.TOPIC_CACHEINTVL_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setMemCacheFlushIntvl((int) result.retData1);
        // get memCacheMsgCntInK parameter value
        if (!WebParameterUtils.getIntParamValue(paramCntr, WebFieldDef.UNFMCACHECNTINK, false,
                (defVal == null ? TBaseConstants.META_VALUE_UNDEFINED : defVal.getMemCacheMsgCntInK()),
                TServerConstants.TOPIC_CACHECNT_INK_MIN, sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setMemCacheMsgCntInK((int) result.retData1);
        // get deletePolicy parameter value
        if (!WebParameterUtils.getDeletePolicyParameter(paramCntr, false,
                (defVal == null ? null : defVal.getDeletePolicy()), sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setDeletePolicy((String) result.retData1);
        // get acceptPublish parameter value
        if (!WebParameterUtils.getBooleanParamValue(paramCntr, WebFieldDef.ACCEPTPUBLISH, false,
                (defVal == null ? null : defVal.getAcceptPublish()), sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setAcceptPublish((Boolean) result.retData1);
        // get acceptSubscribe parameter value
        if (!WebParameterUtils.getBooleanParamValue(paramCntr, WebFieldDef.ACCEPTSUBSCRIBE, false,
                (defVal == null ? null : defVal.getAcceptSubscribe()), sBuffer, result)) {
            return result.isSuccess();
        }
        newConf.setAcceptSubscribe((Boolean) result.retData1);
        result.setSuccResult(newConf);
        return result.isSuccess();
    }

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

    /**
     * Parse the parameter value from an object value to a long value
     *
     * @param paramCntr   parameter container object
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getLongParamValue(T paramCntr, WebFieldDef fieldDef,
                                                boolean required, long defValue,
                                                StringBuilder sBuffer, ProcessResult result) {
        if (!getStringParamValue(paramCntr, fieldDef,
                required, null, sBuffer, result)) {
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
            result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
            sBuffer.delete(0, sBuffer.length());
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param paramCntr   parameter container object
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getIntParamValue(T paramCntr, WebFieldDef fieldDef,
                                               boolean required, StringBuilder sBuffer,
                                               ProcessResult result) {
        return getIntParamValue(paramCntr, fieldDef, required,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                sBuffer, result);
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param paramCntr   parameter container object
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param minValue   min value required
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getIntParamValue(T paramCntr, WebFieldDef fieldDef,
                                               boolean required, int defValue, int minValue,
                                               StringBuilder sBuffer, ProcessResult result) {
        return getIntParamValue(paramCntr, fieldDef, required,
                true, defValue, true, minValue,
                false, TBaseConstants.META_VALUE_UNDEFINED,
                sBuffer, result);
    }

    /**
     * Parse the parameter value from an object value to a integer value
     *
     * @param paramCntr  parameter container object
     * @param fieldDef   the parameter field definition
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param minValue   min value required
     * @param minValue   max value allowed
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getIntParamValue(T paramCntr, WebFieldDef fieldDef,
                                               boolean required, int defValue,
                                               int minValue, int maxValue,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        return getIntParamValue(paramCntr, fieldDef, required, true,
                defValue, true, minValue, true, maxValue, sBuffer, result);
    }

    private static <T> boolean getIntParamValue(T paramCntr,
                                                WebFieldDef fieldDef, boolean required,
                                                boolean hasDefVal, int defValue,
                                                boolean hasMinVal, int minValue,
                                                boolean hasMaxVal, int maxValue,
                                                StringBuilder sBuffer, ProcessResult result) {
        if (!getStringParamValue(paramCntr, fieldDef,
                required, null, sBuffer, result)) {
            return result.success;
        }
        if (fieldDef.isCompFieldType()) {
            Set<Integer> tgtValueSet = new HashSet<>();
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
                        hasMinVal, minValue, hasMaxVal, maxValue, sBuffer, result)) {
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
                    hasMinVal, minValue, hasMaxVal, maxValue, sBuffer, result);
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an object value to a boolean value
     *
     * @param paramCntr   parameter container object
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static <T> boolean getBooleanParamValue(T paramCntr, WebFieldDef fieldDef,
                                                   boolean required, Boolean defValue,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        if (!getStringParamValue(paramCntr, fieldDef,
                required, null, sBuffer, result)) {
            return result.success;
        }
        String paramValue = (String) result.retData1;
        if (paramValue == null) {
            result.setSuccResult(defValue);
            return result.success;
        }
        if (paramValue.equalsIgnoreCase("true")
                || paramValue.equalsIgnoreCase("false")) {
            result.setSuccResult(Boolean.parseBoolean(paramValue));
        } else {
            try {
                result.setSuccResult(!(Long.parseLong(paramValue) == 0));
            } catch (Throwable e) {
                result.setSuccResult(defValue);
            }
        }
        return result.success;
    }

    /**
     * Parse the parameter value from an object value
     *
     * @param paramCntr    parameter container object
     * @param fieldDef     the parameter field definition
     * @param required     a boolean value represent whether the parameter is must required
     * @param defValue     a default value returned if the field not exist
     * @param sBuffer      string buffer
     * @param result       process result
     * @return valid result for the parameter value
     */
    public static <T> boolean getStringParamValue(T paramCntr, WebFieldDef fieldDef,
                                                  boolean required, String defValue,
                                                  StringBuilder sBuffer, ProcessResult result) {
        String paramValue;
        // get parameter value
        if (paramCntr instanceof Map) {
            Map<String, String> keyValueMap =
                    (Map<String, String>) paramCntr;
            paramValue = keyValueMap.get(fieldDef.name);
            if (paramValue == null) {
                paramValue = keyValueMap.get(fieldDef.shortName);
            }
        } else if (paramCntr instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) paramCntr;
            paramValue = req.getParameter(fieldDef.name);
            if (paramValue == null) {
                paramValue = req.getParameter(fieldDef.shortName);
            }
        } else {
            throw new IllegalArgumentException("Unknown parameter type!");
        }
        return checkStrParamValue(paramValue,
                fieldDef, required, defValue, sBuffer, result);
    }

    /**
     * Check the parameter value
     *
     * @param paramValue  parameter value
     * @param fieldDef    the parameter field definition
     * @param required     a boolean value represent whether the parameter is must required
     * @param defValue     a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    private static boolean checkStrParamValue(String paramValue, WebFieldDef fieldDef,
                                              boolean required, String defValue,
                                              StringBuilder sBuffer, ProcessResult result) {
        if (TStringUtils.isNotBlank(paramValue)) {
            // Cleanup value extra characters
            paramValue = escDoubleQuotes(paramValue.trim());
        }
        // Check if the parameter exists
        if (TStringUtils.isBlank(paramValue)) {
            if (required) {
                result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                        .append(" is missing or value is null or blank!").toString());
                sBuffer.delete(0, sBuffer.length());
            } else {
                procStringDefValue(fieldDef.isCompFieldType(), defValue, result);
            }
            return result.success;
        }
        // check if value is norm;
        if (fieldDef.isCompFieldType()) {
            // split original value to items
            TreeSet<String> valItemSet = new TreeSet<>();
            String[] strParamValueItems = paramValue.split(fieldDef.splitToken);
            for (String strParamValueItem : strParamValueItems) {
                if (TStringUtils.isBlank(strParamValueItem)) {
                    continue;
                }
                if (!checkStrValueNorms(fieldDef, strParamValueItem, sBuffer, result)) {
                    return result.success;
                }
                valItemSet.add((String) result.retData1);
            }
            // check if is empty result
            if (valItemSet.isEmpty()) {
                if (required) {
                    result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                            .append(" is missing or value is null or blank!").toString());
                    sBuffer.delete(0, sBuffer.length());
                } else {
                    procStringDefValue(fieldDef.isCompFieldType(), defValue, result);
                }
                return result.success;
            }
            // check max item count
            if (fieldDef.itemMaxCnt != TBaseConstants.META_VALUE_UNDEFINED) {
                if (valItemSet.size() > fieldDef.itemMaxCnt) {
                    result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                            .append("'s item count over max allowed count (")
                            .append(fieldDef.itemMaxCnt).append(")!").toString());
                    sBuffer.delete(0, sBuffer.length());
                }
            }
            valItemSet.comparator();
            result.setSuccResult(valItemSet);
        } else {
            if (!checkStrValueNorms(fieldDef, paramValue, sBuffer, result)) {
                return result.success;
            }
            result.setSuccResult(paramValue);
        }
        return result.success;
    }

    /**
     * Get and valid topicName value
     *
     * @param req        Http Servlet Request
     * @param confManager  configure manager
     * @param required   a boolean value represent whether the parameter is must required
     * @param defValue   a default value returned if the field not exist
     * @param result     process result of parameter value
     * @return process result
     */
    public static boolean getAndValidTopicNameInfo(HttpServletRequest req,
                                                   MetaDataManager confManager,
                                                   boolean required,
                                                   String defValue,
                                                   StringBuilder sBuffer,
                                                   ProcessResult result) {
        if (!WebParameterUtils.getStringParamValue(req,
                WebFieldDef.COMPSTOPICNAME, required, defValue, sBuffer, result)) {
            return result.success;
        }
        Set<String> topicNameSet = (Set<String>) result.retData1;
        Set<String> existedTopicSet =
                confManager.getTotalConfiguredTopicNames();
        for (String topic : topicNameSet) {
            if (!existedTopicSet.contains(topic)) {
                result.setFailResult(sBuffer.append(WebFieldDef.COMPSTOPICNAME.name)
                        .append(" value ").append(topic)
                        .append(" is not configure, please configure first!").toString());
                sBuffer.delete(0, sBuffer.length());
                break;
            }
        }
        return result.success;
    }

    /**
     * check the filter conditions and get them in a String
     *
     * @param paramCntr   parameter container object
     * @param required    denote whether it translate blank condition
     * @param transBlank  whether to translate condition item
     * @param result      process result of parameter value
     * @return process result
     */
    public static <T> boolean getFilterCondString(T paramCntr, boolean required,
                                                  boolean transBlank,
                                                  StringBuilder sBuffer,
                                                  ProcessResult result) {
        if (!getFilterCondSet(paramCntr, required, false, sBuffer, result)) {
            return result.success;
        }
        Set<String> filterCondSet = (Set<String>) result.retData1;
        if (filterCondSet.isEmpty()) {
            if (transBlank) {
                sBuffer.append(TServerConstants.BLANK_FILTER_ITEM_STR);
            }
        } else {
            sBuffer.append(TokenConstants.ARRAY_SEP);
            for (String filterCond : filterCondSet) {
                sBuffer.append(filterCond).append(TokenConstants.ARRAY_SEP);
            }
        }
        result.setSuccResult(sBuffer.toString());
        sBuffer.delete(0, sBuffer.length());
        return result.success;
    }

    /**
     * check the filter conditions and get them in a set
     *
     * @param paramCntr   parameter container object
     * @param required   a boolean value represent whether the parameter is must required
     * @param transCondItem   whether to translate condition item
     * @param result     process result of parameter value
     * @return process result
     */
    public static <T> boolean getFilterCondSet(T paramCntr, boolean required,
                                               boolean transCondItem,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        if (!WebParameterUtils.getStringParamValue(paramCntr,
                WebFieldDef.FILTERCONDS, required, null, sBuffer, result)) {
            return result.success;
        }
        if (transCondItem) {
            // translate filter condition item with "''"
            TreeSet<String> newFilterCondSet = new TreeSet<>();
            Set<String> filterCondSet = (Set<String>) result.retData1;
            if (!filterCondSet.isEmpty()) {
                for (String filterCond : filterCondSet) {
                    newFilterCondSet.add(sBuffer.append(TokenConstants.ARRAY_SEP)
                            .append(filterCond).append(TokenConstants.ARRAY_SEP).toString());
                    sBuffer.delete(0, sBuffer.length());
                }
                newFilterCondSet.comparator();
            }
            result.setSuccResult(newFilterCondSet);
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
     * Parse the parameter value from an json array
     *
     * @param req         Http Servlet Request
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if the field not exist
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static boolean getJsonArrayParamValue(HttpServletRequest req,
                                                 WebFieldDef fieldDef,
                                                 boolean required,
                                                 List<Map<String, String>> defValue,
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
            List<Map<String, String>> arrayValue = new Gson().fromJson(paramValue,
                    new TypeToken<List<Map<String, String>>>(){}.getType());
            result.setSuccResult(arrayValue);
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
     * @param paramCntr   parameter container object
     * @param fieldDef    the parameter field definition
     * @param required    a boolean value represent whether the parameter is must required
     * @param defValue    a default value returned if failed to parse value from the given object
     * @param result      process result
     * @return valid result for the parameter value
     */
    public static <T> boolean getDateParameter(T paramCntr, WebFieldDef fieldDef,
                                               boolean required, Date defValue,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        if (!getStringParamValue(paramCntr, fieldDef,
                required, null, sBuffer, result)) {
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
            result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
            sBuffer.delete(0, sBuffer.length());
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
    public static boolean validReqAuthorizeInfo(HttpServletRequest req, WebFieldDef fieldDef,
                                                boolean required, TMaster master,
                                                StringBuilder sBuffer, ProcessResult result) {
        if (!getStringParamValue(req, fieldDef, required, null, sBuffer, result)) {
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
            TreeSet<String> valItemSet = new TreeSet<>();
            if (TStringUtils.isNotBlank(defValue)) {
                valItemSet.add(defValue);
            }
            valItemSet.comparator();
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
    private static boolean checkStrValueNorms(WebFieldDef fieldDef, String paramVal,
                                              StringBuilder sBuffer, ProcessResult result) {
        paramVal = paramVal.trim();
        if (TStringUtils.isBlank(paramVal)) {
            result.setSuccResult(null);
            return true;
        }
        // check value's max length
        if (fieldDef.valMaxLen != TBaseConstants.META_VALUE_UNDEFINED) {
            if (paramVal.length() > fieldDef.valMaxLen) {
                result.setFailResult(sBuffer.append("over max length for ")
                        .append(fieldDef.name).append(", only allow ")
                        .append(fieldDef.valMaxLen).append(" length").toString());
                sBuffer.delete(0, sBuffer.length());
                return false;
            }
        }
        // check value's pattern
        if (fieldDef.regexCheck) {
            if (!paramVal.matches(fieldDef.regexDef.getPattern())) {
                result.setFailResult(sBuffer.append("illegal value for ")
                        .append(fieldDef.name).append(", value ")
                        .append(fieldDef.regexDef.getErrMsgTemp()).toString());
                sBuffer.delete(0, sBuffer.length());
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
     * @param sBuffer      string buffer
     * @param result   process result
     * @return check result for string value of parameter
     */
    private static boolean checkIntValueNorms(WebFieldDef fieldDef, String paramValue,
                                              boolean hasMinVal, int minValue,
                                              boolean hasMaxVal, int maxValue,
                                              StringBuilder sBuffer, ProcessResult result) {
        try {
            int paramIntVal = Integer.parseInt(paramValue);
            if (hasMinVal && paramIntVal < minValue) {
                result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                        .append(" value must >= ").append(minValue).toString());
                sBuffer.delete(0, sBuffer.length());
                return false;
            }
            if (hasMaxVal && paramIntVal > maxValue) {
                result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                        .append(" value must <= ").append(maxValue).toString());
                sBuffer.delete(0, sBuffer.length());
                return false;
            }
            result.setSuccResult(paramIntVal);
        } catch (Throwable e) {
            result.setFailResult(sBuffer.append("Parameter ").append(fieldDef.name)
                    .append(" parse error: ").append(e.getMessage()).toString());
            sBuffer.delete(0, sBuffer.length());
            return false;
        }
        return true;
    }

    public static boolean checkBrokerInOfflining(int brokerId,
                                                 int manageStatus,
                                                 MetaDataManager metaManager) {
        BrokerSyncStatusInfo brokerSyncStatusInfo =
                metaManager.getBrokerRunSyncStatusInfo(brokerId);
        if ((brokerSyncStatusInfo != null)
                && (brokerSyncStatusInfo.isBrokerRegister())) {
            if ((manageStatus == TStatusConstants.STATUS_MANAGE_OFFLINE)
                    && (brokerSyncStatusInfo.getBrokerRunStatus() != TStatusConstants.STATUS_SERVICE_UNDEFINED)) {
                return true;
            }
        }
        return false;
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
                sb.append(TServerConstants.BLANK_FILTER_ITEM_STR);
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

    public static Tuple2<Boolean, Boolean> getPubSubStatusByManageStatus(int manageStatus) {
        boolean isAcceptPublish = false;
        boolean isAcceptSubscribe = false;
        if (manageStatus >= TStatusConstants.STATUS_MANAGE_ONLINE) {
            if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE) {
                isAcceptPublish = true;
                isAcceptSubscribe = true;
            } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_WRITE) {
                isAcceptPublish = false;
                isAcceptSubscribe = true;
            } else if (manageStatus == TStatusConstants.STATUS_MANAGE_ONLINE_NOT_READ) {
                isAcceptPublish = true;
                isAcceptSubscribe = false;
            }
        }
        return new Tuple2<>(isAcceptPublish, isAcceptSubscribe);
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

    // translate rule info to json format string
    public static <T> int getAndCheckFlowRules(T paramCntr, String defValue,
                                               StringBuilder sBuffer,
                                               ProcessResult result) {
        // get parameter value
        String paramValue;
        // get parameter value
        if (paramCntr instanceof Map) {
            Map<String, String> keyValueMap =
                    (Map<String, String>) paramCntr;
            paramValue = keyValueMap.get(WebFieldDef.FLOWCTRLSET.name);
            if (paramValue == null) {
                paramValue = keyValueMap.get(WebFieldDef.FLOWCTRLSET.shortName);
            }
        } else if (paramCntr instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) paramCntr;
            paramValue = req.getParameter(WebFieldDef.FLOWCTRLSET.name);
            if (paramValue == null) {
                paramValue = req.getParameter(WebFieldDef.FLOWCTRLSET.shortName);
            }
        } else {
            throw new IllegalArgumentException("Unknown parameter type!");
        }
        if (TStringUtils.isBlank(paramValue)) {
            result.setSuccResult(defValue);
            return 0;
        }
        paramValue = paramValue.trim();
        return validFlowRuleValue(paramValue, sBuffer, result);
    }

    private static int validFlowRuleValue(String paramValue,
                                          StringBuilder sBuffer,
                                          ProcessResult result) {
        int ruleCnt = 0;
        paramValue = paramValue.trim();
        List<Integer> ruleTypes = Arrays.asList(0, 1, 2, 3);
        FlowCtrlRuleHandler flowCtrlRuleHandler =
                new FlowCtrlRuleHandler(true);
        Map<Integer, List<FlowCtrlItem>> flowCtrlItemMap;
        try {
            flowCtrlItemMap =
                    flowCtrlRuleHandler.parseFlowCtrlInfo(paramValue);
        } catch (Throwable e) {
            result.setFailResult(sBuffer.append("Parse parameter ")
                    .append(WebFieldDef.FLOWCTRLSET.name)
                    .append(" failure: '").append(e.toString()).toString());
            sBuffer.delete(0, sBuffer.length());
            return 0;
        }
        sBuffer.append("[");
        for (Integer typeId : ruleTypes) {
            if (typeId != null) {
                int rules = 0;
                List<FlowCtrlItem> flowCtrlItems = flowCtrlItemMap.get(typeId);
                if (flowCtrlItems != null) {
                    if (ruleCnt++ > 0) {
                        sBuffer.append(",");
                    }
                    sBuffer.append("{\"type\":").append(typeId.intValue()).append(",\"rule\":[");
                    for (FlowCtrlItem flowCtrlItem : flowCtrlItems) {
                        if (flowCtrlItem != null) {
                            if (rules++ > 0) {
                                sBuffer.append(",");
                            }
                            sBuffer = flowCtrlItem.toJsonString(sBuffer);
                        }
                    }
                    sBuffer.append("]}");
                }
            }
        }
        sBuffer.append("]");
        result.setSuccResult(sBuffer.toString());
        sBuffer.delete(0, sBuffer.length());
        return ruleCnt;
    }
}
