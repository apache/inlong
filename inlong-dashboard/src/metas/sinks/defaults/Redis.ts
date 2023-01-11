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
import { RenderRow } from '@/metas/RenderRow';
import { RenderList } from '@/metas/RenderList';
import i18n from '@/i18n';
import EditableTable from '@/components/EditableTable';
import { sourceFields } from '../common/sourceFields';
import { SinkInfo } from '../common/SinkInfo';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;
const { ColumnDecorator } = RenderList;

const redisTargetTypes = [
  'BOOLEAN',
  'INT',
  'BIGINT',
  'FLOAT',
  'DOUBLE',
  'DATE',
  'DATETIME',
  'CHAR',
  'TIME',
].map(item => ({
  label: item,
  value: item,
}));

export default class RedisSink extends SinkInfo implements DataWithBackend, RenderRow, RenderList {
  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    initialValue: 0,
    tooltip: i18n.t('meta.Sinks.Redis.clusterModeHelp'),
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'cluster',
          value: 'cluster',
        },
        {
          label: 'sentinel',
          value: 'sentinel',
        },
        {
          label: 'standalone',
          value: 'standalone',
        },
      ],
    }),
  })
  @I18n('meta.Sinks.Redis.clusterMode')
  clusterMode: string;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: false }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 0,
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Redis.database')
  database: number;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: false }],
    initialValue: 'PLAIN',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'PLAIN',
          value: 'PLAIN',
        },
        {
          label: 'HASH',
          value: 'HASH',
        },
        {
          label: 'BITMAP',
          value: 'BITMAP',
        },
      ],
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Redis.dataType')
  dataType: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: false }],
    initialValue: 'STATIC_PREFIX_MATCH',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'STATIC_PREFIX_MATCH',
          value: 'STATIC_PREFIX_MATCH',
        },
        {
          label: 'STATIC_KV_PAIR',
          value: 'STATIC_KV_PAIR',
        },
        {
          label: 'DYNAMIC',
          value: 'DYNAMIC',
        },
      ],
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Redis.schemaMapMode')
  schemaMapMode: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: false }],
    initialValue: '',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Redis.password')
  password: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 0,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.Redis.ttlUnit'),
  })
  @I18n('meta.Sinks.Redis.ttl')
  ttl: number;

  @FieldDecorator({
    type: EditableTable,
    rules: [{ required: false }],
    initialValue: [],
    props: values => ({
      size: 'small',
      columns: [
        {
          title: 'Key',
          dataIndex: 'keyName',
          props: {
            disabled: [110, 130].includes(values?.status),
          },
        },
        {
          title: 'Value',
          dataIndex: 'keyValue',
          props: {
            disabled: [110, 130].includes(values?.status),
          },
        },
      ],
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.Redis.ExtList')
  extList: string;

  @FieldDecorator({
    type: EditableTable,
    props: values => ({
      size: 'small',
      editing: ![110, 130].includes(values?.status),
      columns: getFieldListColumns(values),
    }),
  })
  sinkFieldList: Record<string, unknown>[];

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sinks.Redis.Timeout')
  timeout: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sinks.Redis.SoTimeout')
  soTimeout: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sinks.Redis.MaxTotal')
  maxTotal: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sinks.Redis.MaxIdle')
  maxIdle: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sinks.Redis.MinIdle')
  minIdle: number;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    initialValue: 1,
    props: values => ({
      disabled: values?.status === 101,
      min: 1,
    }),
  })
  @I18n('meta.Sinks.Redis.maxRetries')
  maxRetries: number;
}

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `Redis${i18n.t('meta.Sinks.Redis.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.Redis.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `Redis${i18n.t('meta.Sinks.Redis.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: redisTargetTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: redisTargetTypes,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.Redis.IsMetaField'),
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
      title: i18n.t('meta.Sinks.Redis.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
