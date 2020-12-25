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

package org.apache.tubemq.server.common.fielddef;

import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.TokenConstants;
import org.apache.tubemq.corebase.utils.RegexDef;
import org.apache.tubemq.server.common.TServerConstants;
import org.apache.tubemq.server.common.webbase.WebFieldType;


public enum WebFieldDef {

    // Note: Due to compatibility considerations,
    //      the defined fields in the scheme are forbidden to be modified,
    //      only new fields can be added

    TOPICNAME(0, "topicName", "topic", WebFieldType.STRING,
            "Topic name", TBaseConstants.META_MAX_TOPICNAME_LENGTH,
            RegexDef.TMP_STRING),
    GROUPNAME(1, "groupName", "group", WebFieldType.STRING,
            "Group name", TBaseConstants.META_MAX_GROUPNAME_LENGTH,
            RegexDef.TMP_GROUP),
    PARTITIONID(2, "partitionId", "pid", WebFieldType.INT,
            "Partition id", RegexDef.TMP_NUMBER),
    CREATEUSER(3, "createUser", "cur", WebFieldType.STRING,
            "Record creator", TBaseConstants.META_MAX_USERNAME_LENGTH,
            RegexDef.TMP_STRING),
    MODIFYUSER(4, "modifyUser", "mur", WebFieldType.STRING,
            "Record modifier", TBaseConstants.META_MAX_USERNAME_LENGTH,
            RegexDef.TMP_STRING),
    MANUALOFFSET(5, "manualOffset", "offset", WebFieldType.LONG,
            "Reset offset value", RegexDef.TMP_NUMBER),
    MSGCOUNT(6, "msgCount", "cnt", WebFieldType.INT,
            "Number of returned messages", RegexDef.TMP_NUMBER),
    FILTERCONDS(7, "filterConds", "flts", WebFieldType.COMPSTRING,
            "Filter condition items", TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_LENGTH,
            TBaseConstants.CFG_FLT_MAX_FILTER_ITEM_COUNT, RegexDef.TMP_FILTER),
    REQUIREREALOFFSET(8, "requireRealOffset", "dko", WebFieldType.BOOLEAN,
            "Require return disk offset details"),
    NEEDREFRESH(9, "needRefresh", "nrf", WebFieldType.BOOLEAN,
            "Require refresh data"),
    COMPSGROUPNAME(10, "groupName", "group", WebFieldType.COMPSTRING,
            "Group name", TBaseConstants.META_MAX_GROUPNAME_LENGTH,
                   RegexDef.TMP_GROUP),
    COMPSTOPICNAME(11, "topicName", "topic", WebFieldType.COMPSTRING,
            "Topic name", TBaseConstants.META_MAX_TOPICNAME_LENGTH,
            RegexDef.TMP_STRING),
    COMPSPARTITIONID(12, "partitionId", "pid", WebFieldType.COMPINT,
            "Partition id", RegexDef.TMP_NUMBER),
    CALLERIP(13, "callerIp", "cip", WebFieldType.STRING,
            "Caller ip address", TBaseConstants.META_MAX_CLIENT_HOSTNAME_LENGTH),
    BROKERID(14, "brokerId", "brokerId", WebFieldType.INT,
            "Broker ID", RegexDef.TMP_NUMBER),
    COMPSBROKERID(15, "brokerId", "brokerId", WebFieldType.COMPINT,
            "Broker ID", RegexDef.TMP_NUMBER),
    WITHIP(16, "withIp", "ip", WebFieldType.BOOLEAN,
            "Require return ip information, default is false"),
    WITHDIVIDE(17, "divide", "div", WebFieldType.BOOLEAN,
            "Need to divide the returned result, default is false"),
    SRCGROUPNAME(18, "sourceGroupName", "srcGroup", WebFieldType.STRING,
            "Offset clone source group name", TBaseConstants.META_MAX_GROUPNAME_LENGTH,
            RegexDef.TMP_GROUP),
    TGTCOMPSGROUPNAME(19, "targetGroupName", "tgtGroup",
            WebFieldType.COMPSTRING, "Offset clone target group name",
            TBaseConstants.META_MAX_GROUPNAME_LENGTH, RegexDef.TMP_GROUP);




    public final int id;
    public final String name;
    public final String shortName;
    public final WebFieldType type;
    public final String desc;
    public final boolean compVal;
    public final String splitToken;
    public final int itemMaxCnt;
    public final int valMaxLen;
    public final boolean regexCheck;
    public final RegexDef regexDef;


    WebFieldDef(int id, String name, String shortName, WebFieldType type, String desc) {
        this(id, name, shortName, type, desc, TBaseConstants.META_VALUE_UNDEFINED,
                TBaseConstants.META_VALUE_UNDEFINED, false, null);
    }

    WebFieldDef(int id, String name, String shortName, WebFieldType type,
                String desc, int valMaxLen) {
        this(id, name, shortName, type, desc, valMaxLen,
                TBaseConstants.META_VALUE_UNDEFINED, false, null);
    }

    WebFieldDef(int id, String name, String shortName, WebFieldType type,
                String desc, RegexDef regexDef) {
        this(id, name, shortName, type, desc,
                TBaseConstants.META_VALUE_UNDEFINED, regexDef);
    }

    WebFieldDef(int id, String name, String shortName, WebFieldType type,
                String desc, int valMaxLen, RegexDef regexDef) {
        this(id, name, shortName, type, desc, valMaxLen,
                TServerConstants.CFG_BATCH_RECORD_OPERATE_MAX_COUNT,
                true, regexDef);
    }

    WebFieldDef(int id, String name, String shortName, WebFieldType type,
                String desc, int valMaxLen, int itemMaxCnt, RegexDef regexDef) {
        this(id, name, shortName, type, desc, valMaxLen,
                itemMaxCnt, true, regexDef);
    }

    WebFieldDef(int id, String name, String shortName, WebFieldType type,
                String desc, int valMaxLen, int itemMaxCnt,
                boolean regexChk, RegexDef regexDef) {
        this.id = id;
        this.name = name;
        this.shortName = shortName;
        this.type = type;
        this.desc = desc;
        if (isCompFieldType()) {
            this.compVal = true;
            this.splitToken = TokenConstants.ARRAY_SEP;
            this.itemMaxCnt = itemMaxCnt;
        } else {
            this.compVal = false;
            this.splitToken = "";
            this.itemMaxCnt = TBaseConstants.META_VALUE_UNDEFINED;
        }
        this.valMaxLen = valMaxLen;
        this.regexCheck = regexChk;
        this.regexDef = regexDef;
    }

    public boolean isCompFieldType() {
        return (this.type == WebFieldType.COMPINT
                || this.type == WebFieldType.COMPSTRING);
    }

    private static final WebFieldDef[] WEB_FIELD_DEFS;
    private static final int MIN_FIELD_ID = 0;
    public static final int MAX_FIELD_ID;

    static {
        int maxId = -1;
        for (WebFieldDef fieldDef : WebFieldDef.values()) {
            maxId = Math.max(maxId, fieldDef.id);
        }
        WebFieldDef[] idToType = new WebFieldDef[maxId + 1];
        for (WebFieldDef fieldDef : WebFieldDef.values()) {
            idToType[fieldDef.id] = fieldDef;
        }
        WEB_FIELD_DEFS = idToType;
        MAX_FIELD_ID = maxId;
    }

    public static WebFieldDef valueOf(int fieldId) {
        if (fieldId >= MIN_FIELD_ID && fieldId <= MAX_FIELD_ID) {
            return WEB_FIELD_DEFS[fieldId];
        }
        throw new IllegalArgumentException(
                String.format("Unexpected WebFieldDef id `%s`, it should be between `%s` " +
                "and `%s` (inclusive)", fieldId, MIN_FIELD_ID, MAX_FIELD_ID));
    }
}
