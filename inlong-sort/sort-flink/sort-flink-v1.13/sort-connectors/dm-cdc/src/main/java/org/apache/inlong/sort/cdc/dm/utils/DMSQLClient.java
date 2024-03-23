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

package org.apache.inlong.sort.cdc.dm.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.sort.cdc.dm.source.DMConnection;
import org.apache.inlong.sort.cdc.dm.table.DMRecord;
import org.apache.inlong.sort.cdc.dm.table.DMRecord.SourceInfo;
import org.apache.inlong.sort.protocol.ddl.enums.OperationType;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// interact with DBMS_LOGMNR, one connection per client
@Slf4j
public class DMSQLClient {

    public DMConnection connection;
    // the tables involved in incremental changes. for now implement only one table
    private final String tablename;
    private volatile long scn;
    // fetch size per scan
    private static final int BATCH_SIZE = 100000;

    public DMSQLClient(DMConnection globalConnection, String tablename) {
        // before the actual execution, first find the latest scn.
        this.connection = globalConnection;
        this.tablename = tablename;
    }

    public long openlogminer() {
        // a flag to indicate if the logminer is runnable
        boolean runnable = true;

        // before the actual execution, first find the latest scn.
        String querySCN = "select CUR_LSN from v$rlog";
        try {
            connection.query(querySCN,
                    rs -> {
                        // handle the result set
                        while (rs.next()) {
                            log.info("found current lsn: " + rs.getLong("CUR_LSN"));
                            scn = rs.getLong("CUR_LSN");
                        }
                    });
        } catch (Throwable e) {
            log.error("select lsn failed ", e);
            runnable = false;
        }

        log.info("selecting archived logs");
        List<String> paths = new ArrayList<>();
        // then, find all redo log paths. Only those incremental logs which are after current scn are added
        String queryLogPaths =
                "SELECT NAME, FIRST_TIME, NEXT_TIME, FIRST_CHANGE#, NEXT_CHANGE# FROM V$ARCHIVED_LOG WHERE STATUS = 'A'"
                        + "AND NEXT_CHANGE# > " + (scn - 2000000);
        try {
            connection.query(queryLogPaths,
                    rs -> {
                        // handle the result set
                        while (rs.next()) {
                            paths.add(rs.getString("NAME"));
                        }
                    });
        } catch (Throwable e) {
            log.error("query log paths failed ", e);
            runnable = false;
        }

        // add all paths to logminer, path need to have ''
        log.info("adding {} log files", paths.size());
        try {
            connection.setAutoCommit(false);
            for (String path : paths) {
                String addLogFileSql = "DBMS_LOGMNR.ADD_LOGFILE('" + path + "')";
                connection.execute(addLogFileSql);
            }
        } catch (Throwable e) {
            log.error("add logminer file failed ", e);
            runnable = false;
        }

        if (!runnable) {
            return 0;
        }

        log.info("executing start logminer");
        // start log miner, and commit everything.
        String startLogmnrSql = "DBMS_LOGMNR.START_LOGMNR(OPTIONS=>2066)";
        try {
            connection.execute(startLogmnrSql);
            connection.commit();
        } catch (Throwable e) {
            log.error("start logminer failed ", e);
        }

        log.info("finished to open logminer");
        return scn;
    }

    public boolean closelogminer() {
        String closeLogmnrSql = "DBMS_LOGMNR.END_LOGMNR()";
        try {
            connection.execute(closeLogmnrSql);
        } catch (Throwable e) {
            log.error("close logminer failed ", e);
            return false;
        }
        return true;
    }

    // read incremental records and send them downwards.
    public List<DMRecord> processIncrementalRecords(String database, String schema, long scn,
            LogMinerDmlParser parser) {
        ArrayList<DMRecord> list = new ArrayList<>();

        // limit the number of scns read to avoid oom
        String readSCNSql = "SELECT OPERATION, SCN, SQL_REDO, TIMESTAMP, TABLE_NAME FROM "
                + "V$LOGMNR_CONTENTS WHERE TABLE_NAME = '" + tablename + "' AND SCN > " + scn + " LIMIT " + BATCH_SIZE;

        // read and process the records
        try {
            log.info("read scn sql is " + readSCNSql);
            connection.query(readSCNSql,
                    rs -> {
                        while (rs.next()) {
                            DMRecord record = generateDMRecord(database, schema, rs, parser);
                            list.add(record);
                        }
                    });
        } catch (Throwable e) {
            log.error("process incremental snapshot failed ", e);
        }

        log.info("successfully fetched {} incremental records", list.size());
        return list;
    }

    // a helper which generates the actual record given a result set.
    // OPERATION, SCN, SQL_REDO, TIMESTAMP, TABLE_NAME FROM V$LOGMNR_CONTENTS WHERE TABLE_NAME
    private DMRecord generateDMRecord(String database, String schema, ResultSet rs, LogMinerDmlParser parser) {
        try {
            String table = rs.getString("TABLE_NAME");
            long scn = rs.getLong("SCN");
            String operation = rs.getString("OPERATION");
            OperationType opt = generateOperationType(operation);
            SourceInfo sourceInfo = new SourceInfo(database, schema, table, scn);
            // no need for jdbc fields in snapshot phase anymore, instead we need update before & after fields
            // TODO: use metadata to generate the schema
            List<Map<String, Object>> fields = parser.parse(rs.getString("SQL_REDO"), operation);
            DMRecord record = new DMRecord(sourceInfo, opt, fields.get(0), fields.get(1));
            return record;
        } catch (Throwable e) {
            log.error("generate DM record failed ", e);
            return null;
        }
    }

    private OperationType generateOperationType(String operation) {
        switch (operation) {
            case "INSERT":
                return OperationType.INSERT;
            case "DELETE":
                return OperationType.DELETE;
            case "UPDATE":
                return OperationType.UPDATE;
            default:
                throw new RuntimeException("operation type is not supported " + operation);
        }
    }
}
