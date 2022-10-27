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

import { DataWithBackend } from '@/metas/DataWithBackend';
import i18n from '@/i18n';
import EditableTable from '@/components/EditableTable';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';

const { I18n, FormField, TableColumn } = DataWithBackend;

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

export default class HiveSink extends SinkInfo implements DataWithBackend {
  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Clickhouse.DbName')
  dbName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Clickhouse.TableName')
  tableName: string;

  @FormField({
    type: 'radio',
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
  })
  @I18n('meta.Sinks.EnableCreateResource')
  enableCreateResource: number;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Username')
  username: string;

  @FormField({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Password')
  password: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'jdbc:clickhouse://127.0.0.1:8123',
    }),
  })
  @I18n('JDBC URL')
  jdbcUrl: string;

  @FormField({
    type: 'inputnumber',
    initialValue: 1,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.FlushIntervalUnit'),
  })
  @I18n('meta.Sinks.Clickhouse.FlushInterval')
  flushInterval: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 1000,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.FlushRecordUnit'),
  })
  @I18n('meta.Sinks.Clickhouse.FlushRecord')
  flushRecord: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 3,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Clickhouse.RetryTimesUnit'),
  })
  @I18n('meta.Sinks.Clickhouse.RetryTimes')
  retryTime: number;

  @FormField({
    type: 'radio',
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
  })
  @I18n('meta.Sinks.Clickhouse.IsDistributed')
  isDistributed: number;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sinks.Clickhouse.PartitionStrategy')
  partitionStrategy: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.isDistributed && values.partitionStrategy === 'HASH',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Clickhouse.PartitionFields')
  partitionFields: string;

  @FormField({
    type: 'input',
    initialValue: 'Log',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Clickhouse.Engine')
  engine: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Clickhouse.OrderBy')
  orderBy: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Clickhouse.PartitionBy')
  partitionBy: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.Clickhouse.PrimaryKey')
  primaryKey: string;

  @FormField({
    type: EditableTable,
    props: values => ({
      size: 'small',
      editing: ![110, 130].includes(values?.status),
      columns: getFieldListColumns(values),
    }),
  })
  sinkFieldList: Record<string, unknown>[];
}

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
