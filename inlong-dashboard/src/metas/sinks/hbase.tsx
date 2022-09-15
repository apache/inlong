/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import i18n from '@/i18n';
import type { FieldItemType } from '@/metas/common';
import EditableTable from '@/components/EditableTable';
import { sourceFields } from './common/sourceFields';

const hbaseFieldTypes = [
  'int',
  'short',
  'long',
  'float',
  'double',
  'byte',
  'bytes',
  'string',
  'boolean',
].map(item => ({
  label: item,
  value: item,
}));

export const hbase: FieldItemType[] = [
  {
    type: 'input',
    label: i18n.t('meta.Sinks.HBase.Namespace'),
    name: 'namespace',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.HBase.TableName'),
    name: 'tableName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.HBase.RowKey'),
    name: 'rowKey',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.HBase.ZkQuorum'),
    name: 'zkQuorum',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: '127.0.0.1:2181,127.0.0.2:2181',
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.HBase.ZkNodeParent'),
    name: 'zkNodeParent',
    initialValue: '/hbase',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.HBase.BufferFlushMaxSize'),
    name: 'bufferFlushMaxSize',
    initialValue: 2,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    suffix: 'mb',
  },
  {
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.HBase.BufferFlushMaxRows'),
    name: 'bufferFlushMaxRows',
    initialValue: 1000,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
  },
  {
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.HBase.BufferFlushInterval'),
    name: 'bufferFlushInterval',
    initialValue: 1,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    suffix: i18n.t('meta.Sinks.HBase.FlushIntervalUnit'),
  },
  {
    name: 'sinkFieldList',
    type: EditableTable,
    props: values => ({
      size: 'small',
      canDelete: ![110, 130].includes(values?.status),
      columns: getFieldListColumns(values),
    }),
  },
];

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `HBASE${i18n.t('meta.Sinks.HBase.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.HBase.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `HBASE${i18n.t('meta.Sinks.HBase.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: hbaseFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: hbaseFieldTypes,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: 'cfName',
      dataIndex: 'cfName',
      type: 'input',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: 'ttl',
      dataIndex: 'ttl',
      type: 'inputnumber',
      props: (text, record, idx, isNew) => ({
        min: 1,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
  ];
};
