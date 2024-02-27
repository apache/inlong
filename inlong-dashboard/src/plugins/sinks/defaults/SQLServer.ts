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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import i18n from '@/i18n';
import EditableTable from '@/ui/components/EditableTable';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';
import CreateTable from '@/ui/components/CreateTable';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, SyncCreateTableField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

const fieldTypesConf = {
  char: (m, d) => (1 <= m && m <= 8000 ? '' : '1 <= M <= 8000'),
  varchar: (m, d) => (1 <= m && m <= 8000 ? '' : '1 <= M<= 8000'),
  nchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  nvarchar: (m, d) => (1 <= m && m <= 4000 ? '' : '1 <= M <= 4000'),
  text: () => '',
  ntext: () => '',
  xml: () => '',
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  bigserial: (m, d) => (1 <= m && m <= 20 ? '' : '1 <= M <= 20'),
  decimal: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D < M'),
  money: (m, d) => (1 <= m && m <= 15 && 1 <= d && d <= 4 ? '' : '1 <= M <= 15, 1 <= D <= 4'),
  smallmoney: (m, d) => (1 <= m && m <= 7 && 1 <= d && d <= 4 ? '' : '1 <= M <= 7, 1 <= D <= 4'),
  numeric: (m, d) => (1 <= m && m <= 38 && 0 <= d && d < m ? '' : '1 <= M <= 38, 0 <= D <= M'),
  float: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  real: (m, d) => (1 <= m && m <= 24 ? '' : '1 <= M <= 24'),
  bit: (m, d) => (1 <= m && m <= 64 ? '' : '1 <= M <= 64'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1 <= M <= 11'),
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1 <= M <= 4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1 <= M <= 6'),
  time: () => '',
  datetime: () => '',
  datetime2: () => '',
  smalldatetime: () => '',
  datetimeoffset: () => '',
};

const sqlserverFieldTypes = Object.keys(fieldTypesConf).reduce(
  (acc, key) =>
    acc.concat({
      label: key,
      value: key,
    }),
  [],
);

export default class SqlServerSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      placeholder: 'jdbc:sqlserver://127.0.0.1:1433;database=db_name',
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('JDBC URL')
  jdbcUrl: string;

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
  @I18n('meta.Sinks.SQLServer.SchemaName')
  schemaName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    initialValue: 'UTC',
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.SQLServer.ServerTimezone')
  serverTimezone: string;

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
  @I18n('meta.Sinks.SQLServer.TableName')
  tableName: string;

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
  @I18n('meta.Sinks.SQLServer.PrimaryKey')
  primaryKey: string;

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
    type: 'radio',
    rules: [{ required: true }],
    initialValue: true,
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
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.SQLServer.AllMigration')
  allMigration: boolean;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Username')
  username: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.Password')
  password: string;

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
      initialValue: sqlserverFieldTypes[0].value,
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        options: sqlserverFieldTypes,
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [
        { required: true, message: `${i18n.t('meta.Sinks.FieldTypeMessage')}` },
        () => ({
          validator(_, val) {
            if (val) {
              const [, type = val, typeLength = ''] = val.match(/^(.+)\((.+)\)$/) || [];
              if (fieldTypesConf.hasOwnProperty(type)) {
                const [m = -1, d = -1] = typeLength.split(',');
                const errMsg = fieldTypesConf[type]?.(m, d);
                if (typeLength && errMsg) return Promise.reject(new Error(errMsg));
              } else {
                return Promise.reject(new Error('FieldType error'));
              }
            }
            return Promise.resolve();
          },
        }),
      ],
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
      title: i18n.t('meta.Sinks.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
