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

const sqlserverFieldTypes = [
  'char',
  'varchar',
  'nchar',
  'nvarchar',
  'text',
  'ntext',
  'xml',
  'BIGINT',
  'BIGSERIAL',
  'decimal',
  'money',
  'smallmoney',
  'numeric',
  'float',
  'real',
  'bit',
  'int',
  'tinyint',
  'smallint',
  'bigint',
  'time',
  'datetime',
  'datetime2',
  'smalldatetime',
  'datetimeoffset',
].map(item => ({
  label: item,
  value: item,
}));

export const sqlServer: FieldItemType[] = [
  {
    type: 'input',
    label: 'JDBC URL',
    name: 'jdbcUrl',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'jdbc:sqlserver://127.0.0.1:1433;database=db_name',
    }),
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.SQLServer.SchemaName'),
    name: 'schemaName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.SQLServer.ServerTimezone'),
    name: 'serverTimezone',
    initialValue: 'UTC',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.SQLServer.TableName'),
    name: 'tableName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.SQLServer.PrimaryKey'),
    name: 'primaryKey',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'radio',
    label: i18n.t('meta.Sinks.EnableCreateResource'),
    name: 'enableCreateResource',
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
    type: 'radio',
    label: i18n.t('meta.Sinks.SQLServer.AllMigration'),
    name: 'allMigration',
    rules: [{ required: true }],
    initialValue: true,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
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
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.Username'),
    name: 'username',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  },
  {
    type: 'password',
    label: i18n.t('meta.Sinks.Password'),
    name: 'password',
    rules: [{ required: true }],
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
      title: `SQLSERVER${i18n.t('meta.Sinks.SQLServer.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.SQLServer.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `SQLSERVER${i18n.t('meta.Sinks.SQLServer.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: sqlserverFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: sqlserverFieldTypes,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.SQLServer.IsMetaField'),
      initialValue: 0,
      dataIndex: 'isMetaField',
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
      title: i18n.t('meta.Sinks.SQLServer.FieldFormat'),
      dataIndex: 'fieldFormat',
      initialValue: '',
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        options: ['MICROSECONDS', 'MILLISECONDS', 'SECONDS', 'SQL', 'ISO_8601'].map(item => ({
          label: item,
          value: item,
        })),
      }),
      visible: (text, record) =>
        ['BIGINT', 'DATE', 'TIMESTAMP'].includes(record.fieldType as string),
    },
    {
      title: i18n.t('meta.Sinks.SQLServer.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
