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

import i18n from '@/i18n';
import type { FieldItemType } from '@/metas/common';
import { genFields, genForm, genTable } from '@/metas/common';
import { statusList, genStatusTag } from './common/status';

import { hive } from './hive';
import { clickhouse } from './clickhouse';
import { kafka } from './kafka';
import { iceberg } from './iceberg';
import { es } from './es';
import { greenplum } from './greenplum';
import { mysql } from './mysql';
import { oracle } from './oracle';
import { postgreSql } from './postgreSql';
import { sqlServer } from './sqlServer';
import { tdsqlPostgreSQL } from './tdsqlPostgreSql';
import { hbase } from './hbase';

const allSinks = [
  {
    label: 'Hive',
    value: 'HIVE',
    fields: hive,
  },
  {
    label: 'Iceberg',
    value: 'ICEBERG',
    fields: iceberg,
  },
  {
    label: 'ClickHouse',
    value: 'CLICKHOUSE',
    fields: clickhouse,
  },
  {
    label: 'Kafka',
    value: 'KAFKA',
    fields: kafka,
  },
  {
    label: 'Elasticsearch',
    value: 'ELASTICSEARCH',
    fields: es,
  },
  {
    label: 'Greenplum',
    value: 'GREENPLUM',
    fields: greenplum,
  },
  {
    label: 'HBase',
    value: 'HBASE',
    fields: hbase,
  },
  {
    label: 'MySQL',
    value: 'MYSQL',
    fields: mysql,
  },
  {
    label: 'Oracle',
    value: 'ORACLE',
    fields: oracle,
  },
  {
    label: 'PostgreSQL',
    value: 'POSTGRES',
    fields: postgreSql,
  },
  {
    label: 'SQLServer',
    value: 'SQLSERVER',
    fields: sqlServer,
  },
  {
    label: 'TDSQLPostgreSQL',
    value: 'TDSQLPOSTGRESQL',
    fields: tdsqlPostgreSQL,
  },
].sort((a, b) => {
  const a1 = a.label.toUpperCase();
  const a2 = b.label.toUpperCase();
  if (a1 < a2) return -1;
  if (a1 > a2) return 1;
  return 0;
});

const defaultCommonFields: FieldItemType[] = [
  {
    name: 'sinkName',
    type: 'input',
    label: i18n.t('meta.Sinks.SinkName'),
    rules: [
      { required: true },
      {
        pattern: /^[a-zA-Z][a-zA-Z0-9_-]*$/,
        message: i18n.t('meta.Sinks.SinkNameRule'),
      },
    ],
    props: values => ({
      disabled: !!values.id,
      maxLength: 128,
    }),
    _renderTable: true,
  },
  {
    name: 'sinkType',
    type: 'select',
    label: i18n.t('meta.Sinks.SinkType'),
    rules: [{ required: true }],
    initialValue: allSinks[0].value,
    props: values => ({
      dropdownMatchSelectWidth: false,
      disabled: !!values.id,
      options: allSinks,
    }),
  },
  {
    name: 'description',
    type: 'textarea',
    label: i18n.t('meta.Sinks.Description'),
    props: {
      showCount: true,
      maxLength: 300,
    },
  },
  {
    name: 'status',
    type: 'select',
    label: i18n.t('basic.Status'),
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
    visible: false,
    _renderTable: {
      render: text => genStatusTag(text),
    },
  },
];

export const sinks = allSinks.map(item => {
  const itemFields = defaultCommonFields.concat(item.fields);
  const fields = genFields(itemFields);

  return {
    ...item,
    fields,
    form: genForm(fields),
    table: genTable(fields),
    toFormValues: null,
    toSubmitValues: null,
  };
});
