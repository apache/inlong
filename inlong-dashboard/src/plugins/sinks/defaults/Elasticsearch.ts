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
const { FieldDecorator, SyncField, SyncCreateTableField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

const esTypes = [
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

export default class ElasticsearchSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      nodeType: 'ELASTICSEARCH',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  @SyncField()
  @IngestionField()
  dataNodeName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.id === undefined,
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
    suffix: {
      type: 'select',
      name: 'cycle',
      rules: [{ required: true }],
      label: i18n.t('meta.Sinks.ES.Cycle'),
      visible: values => values.index !== undefined,
      props: values => ({
        disabled: [110].includes(values?.status),
        options: [
          {
            label: i18n.t('meta.Sinks.ES.Cycle.Day'),
            value: '_{yyyyMMdd}',
          },
          {
            label: i18n.t('meta.Sinks.ES.Cycle.Hour'),
            value: '_{yyyyMMddHH}',
          },
          {
            label: i18n.t('meta.Sinks.ES.Cycle.Minute'),
            value: '_{yyyyMMddHHmm}',
          },
        ],
      }),
    },
  })
  @I18n('meta.Sinks.ES.Index')
  @IngestionField()
  index: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.id !== undefined,
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @I18n('meta.Sinks.ES.Index')
  @IngestionField()
  indexNamePattern: string;

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
      initialValue: esTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
        options: esTypes,
      }),
      rules: [{ required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` }],
    },
    {
      title: 'Analyzer',
      dataIndex: 'analyzer',
      type: 'input',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'text',
    },
    {
      title: 'Search analyzer',
      dataIndex: 'searchAnalyzer',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'text',
    },
    {
      title: i18n.t('meta.Sinks.ES.DateFormat'),
      dataIndex: 'format',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'date',
    },
    {
      title: 'Scaling factor',
      dataIndex: 'scalingFactor',
      props: (text, record, idx, isNew) => ({
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'scaled_float',
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
