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

package org.apache.inlong.audit.service.entities;

import org.apache.inlong.audit.service.config.Configuration;

import lombok.Data;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.apache.inlong.audit.service.config.SqlConstants.DEFAULT_TABLE_AUDIT_DATA_CHECK_PARTITION_SQL;
import static org.apache.inlong.audit.service.config.SqlConstants.KEY_TABLE_AUDIT_DATA_CHECK_PARTITION_SQL;

@Data
public class PartitionEntity {

    private final String tableName;
    private final String addPartitionStatement;
    private final String deletePartitionStatement;
    private final DateTimeFormatter FORMATTER_YYMMDDHH = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final DateTimeFormatter FORMATTER_YY_MM_DD_HH = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private String formatPartitionName(LocalDate date) {
        return "p" + date.format(FORMATTER_YYMMDDHH);
    }

    public PartitionEntity(String tableName, String addPartitionStatement, String deletePartitionStatement) {
        this.tableName = tableName;
        this.addPartitionStatement = addPartitionStatement;
        this.deletePartitionStatement = deletePartitionStatement;
    }

    public String getAddPartitionSql(long daysToAdd) {
        String partitionValue = LocalDate.now().plusDays(daysToAdd + 1).format(FORMATTER_YY_MM_DD_HH);
        return String.format(addPartitionStatement, getAddPartitionName(daysToAdd), partitionValue);
    }

    public String getDeletePartitionSql(long daysToDelete) {
        return String.format(deletePartitionStatement, getDeletePartitionName(daysToDelete));
    }

    public String getCheckPartitionSql(long partitionDay, boolean isDelete) {
        String partitionName = isDelete ? getDeletePartitionName(partitionDay) : getAddPartitionName(partitionDay);
        return String.format(Configuration.getInstance().get(KEY_TABLE_AUDIT_DATA_CHECK_PARTITION_SQL,
                DEFAULT_TABLE_AUDIT_DATA_CHECK_PARTITION_SQL), tableName, partitionName);
    }

    public String getAddPartitionName(long daysToAdd) {
        return formatPartitionName(LocalDate.now().plusDays(daysToAdd));
    }

    public String getDeletePartitionName(long daysToDelete) {
        return formatPartitionName(LocalDate.now().minusDays(daysToDelete));
    }
}
