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
import { SinkInfo } from '@/plugins/sinks/common/SinkInfo';
import { RenderList } from '@/plugins/RenderList';
import NodeSelect from '@/ui/components/NodeSelect';
import i18n from '@/i18n';
import EditableTable from '@/ui/components/EditableTable';
import {
  fieldTypes,
  fieldTypes as sourceFieldsTypes,
  sourceFields,
} from '@/plugins/sinks/common/sourceFields';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, SyncCreateTableField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class HttpSinkInfo
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
  @SyncField()
  @I18n('meta.Sinks.Http.Path')
  @IngestionField()
  path: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'GET',
    rules: [{ required: true }],
    props: values => ({
      options: [
        {
          label: 'GET',
          value: 'GET',
        },
        {
          label: 'POST',
          value: 'POST',
        },
      ],
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Http.Method')
  method: string;

  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      nodeType: 'HTTP',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  dataNodeName: string;

  @FieldDecorator({
    type: EditableTable,
    props: values => ({
      size: 'small',
      canDelete: record => !(record.id && [110].includes(values?.status)),
      canBatchAdd: true,
      columns: [
        {
          title: i18n.t('meta.Sinks.Http.FieldName'),
          dataIndex: 'fieldName',
          props: (text, record) => ({
            disabled: record.id && [110].includes(values?.status),
          }),
        },
        {
          title: i18n.t('meta.Sinks.Http.FieldValue'),
          dataIndex: 'fieldValue',
          type: 'input',
        },
      ],
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Http.Headers')
  headers: Record<string, string>[];

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: '0',
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Http.MaxRetryTimes')
  maxRetryTimes: number;
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
      initialValue: fieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: fieldTypes,
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` }],
    },
    {
      title: i18n.t('meta.Sinks.Redis.FieldFormat'),
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
