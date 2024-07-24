/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ColumnsType } from 'antd/es/table';

export interface Props {
  inlongGroupId: string;
  inlongStreamId?: string;
  isSource: boolean;
  columns: ColumnsType;
  //   readonly?: string;
}

const clickhouseFieldType = [
  'String',
  'Int8',
  'Int16',
  'Int32',
  'Int64',
  'Float32',
  'Float64',
  'DateTime',
  'Date',
].map(item => ({
  label: item,
  value: item,
}));

const dorisFieldTypes = [
  'null_type',
  'boolean',
  'tinyint',
  'smallint',
  'int',
  'bigint',
  'float',
  'double',
  'date',
  'datetime',
  'decimal',
  'char',
  'largeint',
  'varchar',
  'decimalv2',
  'time',
  'hll',
].map(item => ({
  label: item,
  value: item,
}));

const esFieldTypes = [
  'text',
  'keyword',
  'date',
  'boolean',
  'long',
  'integer',
  'short',
  'byte',
  'double',
  'float',
  'half_float',
  'scaled_float',
].map(item => ({
  label: item,
  value: item,
}));

const hbaseFieldTypes = [
  'int',
  'short',
  'long',
  'float',
  'double',
  'byte',
  'bytes',
  'string',
  'boolean',
].map(item => ({
  label: item,
  value: item,
}));

const hiveFieldTypes = [
  'string',
  'varchar',
  'char',
  'tinyint',
  'smallint',
  'int',
  'bigint',
  'float',
  'double',
  'decimal',
  'numeric',
  'boolean',
  'binary',
  'timestamp',
  'date',
  // 'interval',
].map(item => ({
  label: item,
  value: item,
}));

const hudiFieldTypes = [
  'int',
  'long',
  'string',
  'float',
  'double',
  'date',
  'timestamp',
  'time',
  'boolean',
  'decimal',
  'timestamptz',
  'binary',
  'fixed',
  'uuid',
].map(item => ({
  label: item,
  value: item,
}));

const icebergFieldTypes = [
  'string',
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'decimal',
  'date',
  'time',
  'timestamp',
  'timestamptz',
  'binary',
  'fixed',
  'uuid',
].map(item => ({
  label: item,
  value: item,
}));

const kuduFieldTypes = [
  'int',
  'long',
  'string',
  'float',
  'double',
  'date',
  'timestamp',
  'time',
  'boolean',
  'decimal',
  'timestamptz',
  'binary',
  'fixed',
  'uuid',
].map(item => ({
  label: item,
  value: item,
}));

const redisFieldTypes = [
  'int',
  'long',
  'string',
  'float',
  'double',
  'date',
  'timestamp',
  'time',
  'boolean',
  'timestamptz',
  'binary',
].map(item => ({
  label: item,
  value: item,
}));

const tdsqlPgFieldTypes = [
  'smallint',
  'smallserial',
  'int2',
  'serial2',
  'integer',
  'serial',
  'bigint',
  'bigserial',
  'real',
  'float4',
  'float8',
  'double',
  'numeric',
  'decimal',
  'boolean',
  'date',
  'time',
  'timestamp',
  'char',
  'character',
  'varchar',
  'text',
  'bytea',
].map(item => ({
  label: item,
  value: item,
}));

const greenplumTypesConf = {
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  int2: () => '',
  smallserial: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  serial: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  serial2: () => '',
  integer: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  bigserial: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  real: () => '',
  float4: () => '',
  float8: () => '',
  double: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  numeric: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  decimal: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  boolean: () => '',
  date: () => '',
  time: () => '',
  timestamp: () => '',
  char: (m, d) => (1 <= m && m <= 255 ? '' : '1 <= M <= 255'),
  character: (m, d) => (1 <= m && m <= 255 ? '' : '1 <= M <= 255'),
  varchar: (m, d) => (1 <= m && m <= 255 ? '' : '1 <= M <= 255'),
  text: () => '',
  bytea: () => '',
};

const mysqlTypesConf = {
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1<=M<=4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1<=M<=6'),
  mediumint: (m, d) => (1 <= m && m <= 9 ? '' : '1<=M<=9'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1<=M<=11'),
  float: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1<=M<=20'),
  double: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  numeric: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  decimal: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  boolean: () => '',
  date: () => '',
  time: () => '',
  datetime: () => '',
  char: (m, d) => (1 <= m && m <= 255 ? '' : '1<=M<=255'),
  varchar: (m, d) => (1 <= m && m <= 16383 ? '' : '1<=M<=16383'),
  text: () => '',
  binary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  varbinary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  blob: () => '',
};
const oceanBaseTypesConf = {
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1<=M<=4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1<=M<=6'),
  mediumint: (m, d) => (1 <= m && m <= 9 ? '' : '1<=M<=9'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1<=M<=11'),
  float: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1<=M<=20'),
  double: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  numeric: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  decimal: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  boolean: () => '',
  date: () => '',
  time: () => '',
  datetime: () => '',
  char: (m, d) => (1 <= m && m <= 255 ? '' : '1<=M<=255'),
  varchar: (m, d) => (1 <= m && m <= 16383 ? '' : '1<=M<=16383'),
  text: () => '',
  binary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  varbinary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  blob: () => '',
};
const oracleTypesConf = {
  binary_float: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  binary_double: (m, d) => (1 <= m && m <= 10 ? '' : '1 <= M <= 10'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  float: (m, d) => (1 <= m && m <= 126 ? '' : '1 <= M <= 126'),
  float4: () => '',
  float8: () => '',
  double: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  real: () => '',
  number: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  numeric: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  date: () => '',
  decimal: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  boolean: () => '',
  timestamp: () => '',
  char: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  varchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  clob: () => '',
  raw: (m, d) => (1 <= m && m <= 2000 ? '' : ' 1 <= M <= 2000'),
  blob: () => '',
};

const pgTypesConf = {
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  int2: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  smallserial: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  serial2: () => '',
  integer: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  serial: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  bigserial: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  real: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  float4: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  float8: (m, d) => (24 < m && m <= 53 ? '' : '24 < M <= 53'),
  double: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  numeric: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  decimal: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  boolean: () => '',
  date: () => '',
  time: () => '',
  timestamp: () => '',
  char: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  character: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  varchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  text: () => '',
  bytea: () => '',
};

const sqlServerTypesConf = {
  char: (m, d) => (1 <= m && m <= 8000 ? '' : '1 <= M <= 8000'),
  varchar: (m, d) => (1 <= m && m <= 8000 ? '' : '1 <= M<= 8000'),
  nchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  nvarchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  text: () => '',
  ntext: () => '',
  xml: () => '',
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  bigserial: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  decimal: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  money: (m, d) => (1 <= m && m <= 15 && 1 <= d && d <= 4 ? '' : '1 <= M <= 15, 1 <= D <= 4'),
  smallmoney: (m, d) => (1 <= m && m <= 7 && 1 <= d && d <= 4 ? '' : '1 <= M <= 7, 1 <= D <= 4'),
  numeric: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D <= M'),
  float: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  real: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  bit: (m, d) => (1 <= m && m <= 64 ? '' : '1 <= M <= 64'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1 <= M <= 4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  time: () => '',
  datetime: () => '',
  datetime2: () => '',
  smalldatetime: () => '',
  datetimeoffset: () => '',
};

const starRocksTypesConf = {
  char: (m, d) => (1 <= m && m <= 255 ? '' : '1<=M<=255'),
  varchar: (m, d) => (1 <= m && m <= 255 ? '' : '1<=M<=255'),
  date: () => '',
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1<=M<=4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1<=M<=6'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1<=M<=11'),
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1<=M<=20'),
  largeint: () => '',
  string: () => '',
  datetime: () => '',
  float: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  double: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  decimal: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  boolean: () => '',
};

const getFieldTypes = fieldTypesConf => {
  return Object.keys(fieldTypesConf).reduce(
    (acc, key) =>
      acc.concat({
        label: key,
        value: key,
      }),
    [],
  );
};

export const fieldAllTypes = {
  CLICKHOUSE: clickhouseFieldType,
  DORIS: dorisFieldTypes,
  ELASTICSEARCH: esFieldTypes,
  GREENPLUM: getFieldTypes(greenplumTypesConf),
  HIVE: hiveFieldTypes,
  HBASE: hbaseFieldTypes,
  ICEBERG: icebergFieldTypes,
  HUDI: hudiFieldTypes,
  MYSQL: getFieldTypes(mysqlTypesConf),
  OCEANBASE: getFieldTypes(oceanBaseTypesConf),
  ORACLE: getFieldTypes(oracleTypesConf),
  POSTGRESQL: getFieldTypes(pgTypesConf),
  SQLSERVER: getFieldTypes(sqlServerTypesConf),
  STARROCKS: getFieldTypes(starRocksTypesConf),
  TDSQLPOSTGRESQL: tdsqlPgFieldTypes,
  REDIS: redisFieldTypes,
  KUDU: kuduFieldTypes,
};
