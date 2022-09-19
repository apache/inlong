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

const tdsqlPostgreSQLFieldTypes = [
  'SMALLINT',
  'SMALLSERIAL',
  'INT2',
  'SERIAL2',
  'INTEGER',
  'SERIAL',
  'BIGINT',
  'BIGSERIAL',
  'REAL',
  'FLOAT4',
  'FLOAT8',
  'DOUBLE',
  'NUMERIC',
  'DECIMAL',
  'BOOLEAN',
  'DATE',
  'TIME',
  'TIMESTAMP',
  'CHAR',
  'CHARACTER',
  'VARCHAR',
  'TEXT',
  'BYTEA',
].map(item => ({
  label: item,
  value: item,
}));

export const tdsqlPostgreSQL: FieldItemType[] = [
  {
    type: 'input',
    label: 'JDBC URL',
    name: 'jdbcUrl',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'jdbc:postgresql://127.0.0.1:5432/db_name',
    }),
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.TDSQLPostgreSQL.SchemaName'),
    name: 'schemaName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.TDSQLPostgreSQL.TableName'),
    name: 'tableName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.TDSQLPostgreSQL.PrimaryKey'),
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
      title: `TDSQLPOSTGRESQL${i18n.t('meta.Sinks.TDSQLPostgreSQL.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.TDSQLPostgreSQL.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `TDSQLPOSTGRESQL${i18n.t('meta.Sinks.TDSQLPostgreSQL.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: tdsqlPostgreSQLFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: tdsqlPostgreSQLFieldTypes,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.TDSQLPostgreSQL.IsMetaField'),
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
      title: i18n.t('meta.Sinks.TDSQLPostgreSQL.FieldFormat'),
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
      title: i18n.t('meta.Sinks.TDSQLPostgreSQL.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
