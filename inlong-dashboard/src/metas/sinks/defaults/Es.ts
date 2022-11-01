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
import EditableTable from '@/components/EditableTable';
import { sourceFields } from '../common/sourceFields';
import { SinkInfo } from '../common/SinkInfo';
import { DataWithBackend } from '@/metas/DataWithBackend';

const { I18n, FormField, TableColumn } = DataWithBackend;

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

export default class EsSink extends SinkInfo implements DataWithBackend {
  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.IndexName')
  indexName: string;

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
  @TableColumn()
  @I18n('meta.Sinks.Username')
  username: string;

  @FormField({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Password')
  password: string;

  @FormField({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.Host')
  host: string;

  @FormField({
    type: 'inputnumber',
    initialValue: 9200,
    rules: [{ required: true }],
    props: values => ({
      min: 1,
      max: 65535,
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.Port')
  port: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 1,
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Es.FlushIntervalUnit'),
    props: values => ({
      min: 1,
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.FlushInterval')
  flushInterval: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 1000,
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Es.FlushRecordUnit'),
    props: values => ({
      min: 1,
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.FlushRecord')
  flushRecord: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 3,
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Es.RetryTimesUnit'),
    props: values => ({
      min: 1,
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.Es.RetryTimes')
  retryTime: number;

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
      title: `ES ${i18n.t('meta.Sinks.Es.FieldName')}`,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.Es.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ES ${i18n.t('meta.Sinks.Es.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: esTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
        options: esTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: 'Analyzer',
      dataIndex: 'analyzer',
      type: 'input',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'text',
    },
    {
      title: 'SearchAnalyzer',
      dataIndex: 'searchAnalyzer',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'text',
    },
    {
      title: i18n.t('meta.Sinks.Es.DateFormat'),
      dataIndex: 'format',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'date',
    },
    {
      title: 'ScalingFactor',
      dataIndex: 'scalingFactor',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      visible: (text, record) => record.fieldType === 'scaled_float',
    },
    {
      title: `ES ${i18n.t('meta.Sinks.Es.FieldDescription')}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
  ];
};
