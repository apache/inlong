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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import i18n from '@/i18n';
import EditableTable from '@/ui/components/EditableTable';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';
import NodeSelect from '@/ui/components/NodeSelect';
import CreateTable from '@/ui/components/CreateTable';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, SyncCreateTableField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

const clickHouseTargetTypes = [
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

export default class ClickHouseSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.ClickHouse.DbName')
  dbName: string;

  @FieldDecorator({
    type: CreateTable,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      sinkType: values.sinkType,
      inlongGroupId: values.inlongGroupId,
      inlongStreamId: values.inlongStreamId,
      fieldName: 'tableName',
      sinkObj: {
        ...values,
      },
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.ClickHouse.TableName')
  tableName: string;

  @FieldDecorator({
    type: 'radio',
    rules: [{ required: true }],
    initialValue: 1,
    tooltip: i18n.t('meta.Sinks.EnableCreateResourceHelp'),
    props: values => ({
      disabled: [110].includes(values?.status),
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
  @IngestionField()
  @I18n('meta.Sinks.EnableCreateResource')
  enableCreateResource: number;

  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      nodeType: 'CLICKHOUSE',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  dataNodeName: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 1,
    props: values => ({
      disabled: [110].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.ClickHouse.FlushIntervalUnit'),
  })
  @I18n('meta.Sinks.ClickHouse.FlushInterval')
  @SyncCreateTableField()
  @ColumnDecorator()
  @IngestionField()
  flushInterval: number;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 1000,
    props: values => ({
      disabled: [110].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.ClickHouse.FlushRecordUnit'),
  })
  @I18n('meta.Sinks.ClickHouse.FlushRecord')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  flushRecord: number;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 3,
    props: values => ({
      disabled: [110].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.ClickHouse.RetryTimesUnit'),
  })
  @I18n('meta.Sinks.ClickHouse.RetryTimes')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  retryTimes: number;

  @FieldDecorator({
    type: 'radio',
    initialValue: 0,
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.ClickHouse.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Sinks.ClickHouse.No'),
          value: 0,
        },
      ],
    }),
    rules: [{ required: true }],
  })
  @I18n('meta.Sinks.ClickHouse.IsDistributed')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  isDistributed: number;

  @FieldDecorator({
    type: 'select',
    initialValue: 'BALANCE',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
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
  @I18n('meta.Sinks.ClickHouse.PartitionStrategy')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  partitionStrategy: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.isDistributed && values.partitionStrategy === 'HASH',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PartitionFields')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  partitionFields: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'Log',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: 'Log',
          value: 'Log',
        },
        {
          label: 'MergeTree',
          value: 'MergeTree',
        },
        {
          label: 'ReplicatedMergeTree',
          value: 'ReplicatedMergeTree',
        },
      ],
    }),
  })
  @I18n('meta.Sinks.ClickHouse.Engine')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  engine: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.OrderBy')
  @SyncCreateTableField()
  @IngestionField()
  orderBy: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PartitionBy')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  partitionBy: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.PrimaryKey')
  @SyncField()
  @IngestionField()
  primaryKey: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.ClickHouse.Cluster')
  @SyncCreateTableField()
  @ColumnDecorator()
  @IngestionField()
  cluster: string;

  @FieldDecorator({
    type: 'inputnumber',
    visible: values => values.engine === 'MergeTree' || values.engine === 'ReplicatedMergeTree',
    suffix: {
      type: 'select',
      name: 'ttlUnit',
      props: values => ({
        disabled: [110].includes(values?.status),
        options: [
          {
            label: 'Second',
            value: 'second',
          },
          {
            label: 'Minute',
            value: 'minute',
          },
          {
            label: 'Hour',
            value: 'hour',
          },
          {
            label: 'Day',
            value: 'day',
          },
          {
            label: 'Week',
            value: 'week',
          },
          {
            label: 'Month',
            value: 'month',
          },
          {
            label: 'Quarter',
            value: 'quarter',
          },
          {
            label: 'Year',
            value: 'year',
          },
        ],
      }),
    },
    props: values => ({
      min: 1,
      precision: 0,
      disabled: [110].includes(values?.status),
    }),
  })
  @I18n('Time To Live')
  @ColumnDecorator()
  @IngestionField()
  @SyncCreateTableField()
  ttl: number;

  @FieldDecorator({
    type: EditableTable,
    props: values => ({
      size: 'small',
      editing: ![110].includes(values?.status),
      columns: getFieldListColumns(values),
      canBatchAdd: true,
      upsertByFieldKey: true,
    }),
  })
  @IngestionField()
  sinkFieldList: Record<string, unknown>[];

  @FieldDecorator({
    type: EditableTable,
    initialValue: [],
    props: values => ({
      size: 'small',
      editing: ![110].includes(values?.status),
      columns: getFieldListColumns(values).filter(
        item => item.dataIndex !== 'sourceFieldName' && item.dataIndex !== 'sourceFieldType',
      ),
      canBatchAdd: true,
      upsertByFieldKey: true,
    }),
  })
  @SyncCreateTableField()
  createTableField: Record<string, unknown>[];
}

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: i18n.t('meta.Sinks.SinkFieldName'),
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z_][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.SinkFieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: i18n.t('meta.Sinks.SinkFieldType'),
      dataIndex: 'fieldType',
      initialValue: clickHouseTargetTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
        options: clickHouseTargetTypes,
      }),
      rules: [{ required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` }],
    },
    {
      title: 'Default type',
      dataIndex: 'defaultType',
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
        options: ['DEFAULT', 'EPHEMERAL', 'MATERIALIZED', 'ALIAS'].map(item => ({
          label: item,
          value: item,
        })),
      }),
    },
    {
      title: 'Default expr',
      dataIndex: 'defaultExpr',
      type: 'input',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) =>
        ['DEFAULT', 'EPHEMERAL', 'MATERIALIZED', 'ALIAS'].includes(record.defaultType as string),
    },
    {
      title: i18n.t('meta.Sinks.ClickHouse.CompressionCode'),
      dataIndex: 'compressionCode',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: i18n.t('meta.Sinks.ClickHouse.TtlExpr'),
      dataIndex: 'ttlExpr',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: i18n.t('meta.Sinks.FieldDescription'),
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
    },
  ];
};
