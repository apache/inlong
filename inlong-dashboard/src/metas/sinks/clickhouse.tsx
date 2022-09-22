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
import EditableTable from '@/components/EditableTable';
import { sourceFields } from './common/sourceFields';

const clickhouseTargetTypes = [
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

export const clickhouse: FieldItemType[] = [
  {
    name: 'dbName',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.DbName'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'tableName',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.TableName'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'enableCreateResource',
    type: 'radio',
    label: i18n.t('meta.Sinks.EnableCreateResource'),
    rules: [{ required: true }],
    initialValue: 1,
    tooltip: i18n.t('meta.Sinks.EnableCreateResourceHelp'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: i18n.t('basic.Yes'),
          value: 1,
        },
        {
          label: i18n.t('basic.No'),
          value: 0,
        },
      ],
    }),
  },
  {
    name: 'username',
    type: 'input',
    label: i18n.t('meta.Sinks.Username'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'password',
    type: 'password',
    label: i18n.t('meta.Sinks.Password'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    type: 'input',
    label: 'JDBC URL',
    name: 'jdbcUrl',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'jdbc:clickhouse://127.0.0.1:8123',
    }),
  },
  {
    name: 'flushInterval',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.Clickhouse.FlushInterval'),
    initialValue: 1,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.FlushIntervalUnit'),
  },
  {
    name: 'flushRecord',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.Clickhouse.FlushRecord'),
    initialValue: 1000,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.FlushRecordUnit'),
  },
  {
    name: 'retryTime',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.Clickhouse.RetryTimes'),
    initialValue: 3,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.RetryTimesUnit'),
  },
  {
    name: 'isDistributed',
    type: 'radio',
    label: i18n.t('meta.Sinks.Clickhouse.IsDistributed'),
    initialValue: 0,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.Clickhouse.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Sinks.Clickhouse.No'),
          value: 0,
        },
      ],
    }),
    rules: [{ required: true }],
  },
  {
    name: 'partitionStrategy',
    type: 'select',
    label: i18n.t('meta.Sinks.Clickhouse.PartitionStrategy'),
    initialValue: 'BALANCE',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'BALANCE',
          value: 'BALANCE',
        },
        {
          label: 'RANDOM',
          value: 'RANDOM',
        },
        {
          label: 'HASH',
          value: 'HASH',
        },
      ],
    }),
    visible: values => values.isDistributed,
  },
  {
    name: 'partitionFields',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.PartitionFields'),
    rules: [{ required: true }],
    visible: values => values.isDistributed && values.partitionStrategy === 'HASH',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    name: 'engine',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.Engine'),
    initialValue: 'Log',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    name: 'orderBy',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.OrderBy'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    name: 'partitionBy',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.PartitionBy'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    name: 'primaryKey',
    type: 'input',
    label: i18n.t('meta.Sinks.Clickhouse.PrimaryKey'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    name: 'sinkFieldList',
    type: EditableTable,
    props: values => ({
      size: 'small',
      editing: ![110, 130].includes(values?.status),
      columns: getFieldListColumns(values),
    }),
  },
];

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `ClickHouse${i18n.t('meta.Sinks.Clickhouse.FieldName')}`,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.Clickhouse.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.Clickhouse.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: clickhouseTargetTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
        options: clickhouseTargetTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: 'DefaultType',
      dataIndex: 'defaultType',
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
        options: ['DEFAULT', 'EPHEMERAL', 'MATERIALIZED', 'ALIAS'].map(item => ({
          label: item,
          value: item,
        })),
      }),
    },
    {
      title: 'DefaultExpr',
      dataIndex: 'defaultExpr',
      type: 'input',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) =>
        ['DEFAULT', 'EPHEMERAL', 'MATERIALIZED', 'ALIAS'].includes(record.defaultType as string),
    },
    {
      title: i18n.t('meta.Sinks.Clickhouse.CompressionCode'),
      dataIndex: 'compressionCode',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: i18n.t('meta.Sinks.Clickhouse.TtlExpr'),
      dataIndex: 'ttlExpr',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.Clickhouse.FieldDescription')}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
  ];
};
