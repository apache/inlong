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
import { sourceFields } from '../common/sourceFields';
import { SinkInfo } from '../common/SinkInfo';
import NodeSelect from '@/ui/components/NodeSelect';
import CreateTable from '@/ui/components/CreateTable';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, SyncMoveDbField, SyncCreateTableField, IngestionField } =
  RenderRow;
const { ColumnDecorator } = RenderList;

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

const matchPartitionStrategies = fieldType => {
  const data = [
    {
      label: 'None',
      value: 'None',
      disabled: false,
    },
    {
      label: 'Identity',
      value: 'Identity',
      disabled: false,
    },
    {
      label: 'Year',
      value: 'Year',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Month',
      value: 'Month',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Day',
      value: 'Day',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Hour',
      value: 'Hour',
      disabled: fieldType !== 'timestamp',
    },
    {
      label: 'Bucket',
      value: 'Bucket',
      disabled: ![
        'string',
        'boolean',
        'short',
        'int',
        'long',
        'float',
        'double',
        'decimal',
      ].includes(fieldType),
    },
    {
      label: 'Truncate',
      value: 'Truncate',
      disabled: !['string', 'int', 'long', 'binary', 'decimal'].includes(fieldType),
    },
  ];

  return data.filter(item => !item.disabled);
};

export default class IcebergSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  readonly id: number;

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
  @I18n('meta.Sinks.Iceberg.DbName')
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
  @I18n('meta.Sinks.Iceberg.TableName')
  tableName: string;

  @FieldDecorator({
    type: 'radiobutton',
    initialValue: '${database}',
    tooltip: i18n.t('meta.Sinks.Iceberg.PatternHelp'),
    rules: [{ required: true }],
    props: values => ({
      size: 'middle',
      disabled: [110].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.Iceberg.Options.DBSameName'),
          value: '${database}',
          disabled: Boolean(values.id),
        },
        {
          label: i18n.t('meta.Sinks.Iceberg.Options.Customize'),
          value: 'false',
          disabled: Boolean(values.id),
        },
      ],
    }),
    suffix: {
      type: 'input',
      name: 'databasePattern',
      visible: values =>
        values.backupDatabase === 'false' ||
        (values.id !== undefined && values.databasePattern !== '${database}'),
      props: values => ({
        style: { width: 100 },
        disabled: [110].includes(values?.status),
      }),
    },
  })
  @SyncMoveDbField()
  @I18n('meta.Sinks.Iceberg.DatabasePattern')
  backupDatabase: string;

  @FieldDecorator({
    type: 'radiobutton',
    initialValue: '${table}',
    rules: [{ required: true }],
    tooltip: i18n.t('meta.Sinks.Iceberg.PatternHelp'),
    props: values => ({
      size: 'middle',
      options: [
        {
          label: i18n.t('meta.Sinks.Iceberg.Options.TableSameName'),
          value: '${table}',
          disabled: Boolean(values.id),
        },
        {
          label: i18n.t('meta.Sinks.Iceberg.Options.Customize'),
          value: 'false',
          disabled: Boolean(values.id),
        },
      ],
    }),
    suffix: {
      type: 'input',
      name: 'tablePattern',
      visible: values =>
        values.backupTable === 'false' ||
        (values.id !== undefined && values.tablePattern !== '${table}'),
      props: values => ({
        style: { width: 100 },
        disabled: [110].includes(values?.status),
      }),
    },
  })
  @SyncMoveDbField()
  @I18n('meta.Sinks.Iceberg.TablePattern')
  backupTable: string;

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
      nodeType: 'ICEBERG',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  dataNodeName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    initialValue: 'Parquet',
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: 'Parquet',
          value: 'Parquet',
        },
        {
          label: 'Orc',
          value: 'Orc',
        },
        {
          label: 'Avro',
          value: 'Avro',
        },
      ],
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Iceberg.FileFormat')
  fileFormat: string;

  @FieldDecorator({
    type: 'radio',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: 'append',
          value: 'APPEND',
        },
        {
          label: 'upsert',
          value: 'UPSERT',
        },
      ],
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Iceberg.AppendMode')
  appendMode: string;

  @FieldDecorator({
    type: EditableTable,
    initialValue: [],
    props: values => ({
      size: 'small',
      columns: [
        {
          title: 'Key',
          dataIndex: 'keyName',
          props: {
            disabled: [110].includes(values?.status),
          },
        },
        {
          title: 'Value',
          dataIndex: 'keyValue',
          props: {
            disabled: [110].includes(values?.status),
          },
        },
      ],
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Iceberg.ExtList')
  extList: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    initialValue: 'EXACTLY_ONCE',
    isPro: true,
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: 'EXACTLY_ONCE',
          value: 'EXACTLY_ONCE',
        },
        {
          label: 'AT_LEAST_ONCE',
          value: 'AT_LEAST_ONCE',
        },
      ],
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Iceberg.DataConsistency')
  dataConsistency: string;

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
      width: 110,
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
      width: 130,
      initialValue: icebergFieldTypes[0].value,
      type: 'select',
      rules: [{ required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` }],
      props: (text, record, idx, isNew) => ({
        options: icebergFieldTypes,
        onChange: value => {
          const partitionStrategies = matchPartitionStrategies(value);
          if (partitionStrategies.every(item => item.value !== record.partitionStrategy)) {
            return {
              partitionStrategy: partitionStrategies[0].value,
            };
          }
        },
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: 'Length',
      dataIndex: 'fieldLength',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'fixed',
    },
    {
      title: 'Precision',
      dataIndex: 'fieldPrecision',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'decimal',
    },
    {
      title: 'Scale',
      dataIndex: 'fieldScale',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'decimal',
    },
    {
      title: i18n.t('meta.Sinks.Iceberg.PartitionStrategy'),
      dataIndex: 'partitionStrategy',
      type: 'select',
      initialValue: 'None',
      rules: [{ required: true }],
      props: (text, record) => ({
        options: matchPartitionStrategies(record.fieldType),
      }),
    },
    {
      title: i18n.t('meta.Sinks.FieldDescription'),
      dataIndex: 'fieldComment',
    },
  ];
};
