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

import type { GetStorageFormFieldsType, GetStorageColumnsType } from '@/utils/metaData';
import type { ColumnsType } from 'antd/es/table';
import { StorageHive } from './StorageHive';
import { StorageClickhouse } from './StorageClickhouse';
import { StorageKafka } from './StorageKafka';
import { StorageIceberg } from './StorageIceberg';
import { StorageEs } from './StorageEs';
import { StorageGreenplum } from './StorageGreenplum';
import { StorageMySQL } from './StorageMySQL';
import { StorageOracle } from './StorageOracle';
import { StoragePostgreSQL } from './StoragePostgreSQL';
import { StorageSQLServer } from './StorageSQLServer';
import { StorageTDSQLPostgreSQL } from './StorageTDSQLPostgreSQL';
import { StorageHBase } from './StorageHBase';

export interface StoragesType {
  label: string;
  value: string;
  // Generate form configuration for single data
  getForm: GetStorageFormFieldsType;
  // Generate table display configuration
  tableColumns: ColumnsType;
  // Detailed mapping data field configuration for this type of flow
  getFieldListColumns?: GetStorageColumnsType;
  // Custom convert interface data to front-end data format
  toFormValues?: (values: unknown) => unknown;
  // Custom convert front-end data to interface data format
  toSubmitValues?: (values: unknown) => unknown;
}

export const Storages: StoragesType[] = [
  {
    label: 'Hive',
    value: 'Hive',
    ...StorageHive,
  },
  {
    label: 'Iceberg',
    value: 'Iceberg',
    ...StorageIceberg,
  },
  {
    label: 'ClickHouse',
    value: 'ClickHouse',
    ...StorageClickhouse,
  },
  {
    label: 'Kafka',
    value: 'Kafka',
    ...StorageKafka,
  },
  {
    label: 'Elasticsearch',
    value: 'Elasticsearch',
    ...StorageEs,
  },
  {
    label: 'Greenplum',
    value: 'Greenplum',
    ...StorageGreenplum,
  },
  {
    label: 'HBase',
    value: 'HBase',
    ...StorageHBase,
  },
  {
    label: 'MySQL',
    value: 'MySQL',
    ...StorageMySQL,
  },
  {
    label: 'Oracle',
    value: 'Oracle',
    ...StorageOracle,
  },
  {
    label: 'PostgreSQL',
    value: 'PostgreSQL',
    ...StoragePostgreSQL,
  },
  {
    label: 'SQLServer',
    value: 'SQLServer',
    ...StorageSQLServer,
  },
  {
    label: 'TDSQLPostgreSQL',
    value: 'TDSQLPostgreSQL',
    ...StorageTDSQLPostgreSQL,
  },
];
