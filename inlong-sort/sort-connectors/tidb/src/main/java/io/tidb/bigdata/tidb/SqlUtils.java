/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.tidb;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;

public class SqlUtils {

  public static final String QUERY_PD_SQL =
      "SELECT `INSTANCE` FROM `INFORMATION_SCHEMA`.`CLUSTER_INFO` WHERE `TYPE` = 'pd'";
  public static final String QUERY_CLUSTERED_INDEX_SQL_FORMAT =
      "SELECT `TIDB_PK_TYPE` FROM `INFORMATION_SCHEMA`.`TABLES` "
          + "WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s';";
  public static final String TIDB_ROW_FORMAT_VERSION_SQL = "SELECT @@tidb_row_format_version";

  private static List<String> concatNameType(List<String> columnNames, List<String> columnTypes,
      List<String> primaryKeyColumns, List<String> uniqueKeyColumns) {
    List<String> nameType = new ArrayList<>(columnNames.size() + 1);
    for (int i = 0; i < columnNames.size(); i++) {
      nameType.add(format("`%s` %s", columnNames.get(i), columnTypes.get(i)));
    }
    if (primaryKeyColumns != null && primaryKeyColumns.size() != 0) {
      nameType.add(format("PRIMARY KEY(%s)",
          primaryKeyColumns.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(","))));
    }
    if (uniqueKeyColumns != null && uniqueKeyColumns.size() != 0) {
      nameType.add(format("UNIQUE KEY(%s)",
          uniqueKeyColumns.stream().map(uk -> "`" + uk + "`").collect(Collectors.joining(","))));
    }
    return nameType;
  }

  public static String getCreateTableSql(String databaseName, String tableName,
      List<String> columnNames, List<String> columnTypes, List<String> primaryKeyColumns,
      List<String> uniqueKeyColumns, boolean ignoreIfExists) {
    return format("CREATE TABLE %s `%s`.`%s`(\n%s\n)",
        ignoreIfExists ? "IF NOT EXISTS" : "",
        databaseName,
        tableName,
        join(",\n", concatNameType(columnNames, columnTypes, primaryKeyColumns, uniqueKeyColumns))
    );
  }

  public static String getInsertSql(String databaseName, String tableName,
      List<String> columnNames) {
    return format(
        "INSERT INTO `%s`.`%s`(%s) VALUES(%s)",
        databaseName,
        tableName,
        columnNames.stream().map(name -> format("`%s`", name)).collect(Collectors.joining(",")),
        join(",", nCopies(columnNames.size(), "?"))
    );
  }

  public static String getUpsertSql(String databaseName, String tableName,
      List<String> columnNames) {
    String insertSql = getInsertSql(databaseName, tableName, columnNames);
    return format("%s ON DUPLICATE KEY UPDATE %s", insertSql,
        columnNames.stream().map(columnName -> format("`%s`=VALUES(`%s`)", columnName, columnName))
            .collect(Collectors.joining(",")));
  }

  public static List<TiIndexInfo> getUniqueIndexes(TiTableInfo tiTableInfo,
      boolean ignoreAutoincrementColumn) {
    List<TiIndexInfo> uniqueIndexes = tiTableInfo.getIndices()
        .stream()
        .filter(TiIndexInfo::isUnique)
        .collect(Collectors.toList());
    Optional<String> autoIncrementColumn = Optional
        .ofNullable(tiTableInfo.getAutoIncrementColInfo())
        .map(TiColumnInfo::getName);
    if (autoIncrementColumn.isPresent() && ignoreAutoincrementColumn) {
      uniqueIndexes = uniqueIndexes.stream()
          .filter(tiIndexInfo -> tiIndexInfo.getIndexColumns().stream()
              .noneMatch(column -> column.getName().equals(autoIncrementColumn.get())))
          .collect(Collectors.toList());
    }
    return uniqueIndexes;
  }

  public static String printColumnMapping(String[] columns1, String[] columns2) {
    return IntStream.range(0, Math.max(columns1.length, columns2.length))
        .mapToObj(i -> String.format("`%s` -> `%s`",
            i < columns1.length ? columns1[i] : "  ",
            i < columns2.length ? columns2[i] : "  "))
        .collect(Collectors.joining(",\n"));
  }

}
