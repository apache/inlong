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

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, IngestionField, SyncMoveDbField } = RenderRow;
const { ColumnDecorator } = RenderList;

const dorisTargetTypes = [
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
  'largenint',
  'varchar',
  'decimalv2',
  'time',
  'hll',
].map(item => ({
  label: item,
  value: item,
}));

export default class DorisSink extends SinkInfo implements DataWithBackend, RenderRow, RenderList {
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Doris.HttpAddress')
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  feNodes: string;

  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      nodeType: 'DORIS',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  dataNodeName: string;

  @FieldDecorator({
    type: 'radiobutton',
    initialValue: '${database}',
    tooltip: i18n.t('meta.Sinks.Doris.PatternHelp'),
    rules: [{ required: true }],
    props: values => ({
      size: 'middle',
      disabled: [110].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.Doris.Options.DBSameName'),
          value: '${database}',
          disabled: Boolean(values.id),
        },
        {
          label: i18n.t('meta.Sinks.Doris.Options.Customize'),
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
  @I18n('meta.Sinks.Doris.DatabaseNamePattern')
  backupDatabase: string;

  @FieldDecorator({
    type: 'radiobutton',
    initialValue: '${table}',
    rules: [{ required: true }],
    tooltip: i18n.t('meta.Sinks.Doris.PatternHelp'),
    props: values => ({
      size: 'middle',
      options: [
        {
          label: i18n.t('meta.Sinks.Doris.Options.TableSameName'),
          value: '${table}',
          disabled: Boolean(values.id),
        },
        {
          label: i18n.t('meta.Sinks.Doris.Options.Customize'),
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
  @I18n('meta.Sinks.Doris.TableNamePattern')
  backupTable: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Doris.TableIdentifier')
  @SyncField()
  @IngestionField()
  tableIdentifier: string;

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
  @I18n('meta.Sinks.Doris.LabelPrefix')
  labelPrefix: string;

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
  @I18n('meta.Sinks.Doris.PrimaryKey')
  primaryKey: string;

  @FieldDecorator({
    type: 'radio',
    rules: [{ required: true }],
    initialValue: false,
    props: values => ({
      disabled: [110].includes(values?.status),
      options: [
        {
          label: i18n.t('basic.Yes'),
          value: true,
        },
        {
          label: i18n.t('basic.No'),
          value: false,
        },
      ],
    }),
  })
  @I18n('meta.Sinks.Doris.SinkMultipleEnable')
  @SyncField()
  @IngestionField()
  sinkMultipleEnable: boolean;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Doris.SinkMultipleFormat')
  @SyncField()
  @IngestionField()
  sinkMultipleFormat: string;

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
  @I18n('meta.Sinks.Doris.DatabasePattern')
  databasePattern: string;

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
  @I18n('meta.Sinks.Doris.TablePattern')
  tablePattern: string;

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
}

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: i18n.t('meta.Sinks.SinkFieldName'),
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z_][0-9a-z_]*$/,
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
      initialValue: dorisTargetTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: dorisTargetTypes,
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` }],
    },
    {
      title: i18n.t('meta.Sinks.Doris.IsMetaField'),
      dataIndex: 'isMetaField',
      initialValue: 0,
      type: 'select',
      props: (text, record, idx, isNew) => ({
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
      title: i18n.t('meta.Sinks.Doris.FieldFormat'),
      dataIndex: 'fieldFormat',
      initialValue: 0,
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        options: ['MICROSECONDS', 'MILLISECONDS', 'SECONDS', 'SQL', 'ISO_8601'].map(item => ({
          label: item,
          value: item,
        })),
      }),
      visible: (text, record) => ['BIGINT', 'DATE'].includes(record.fieldType as string),
    },
    {
      title: i18n.t('meta.Sinks.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
