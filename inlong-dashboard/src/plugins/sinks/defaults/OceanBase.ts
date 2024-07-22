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
import { sourceFields } from '../common/sourceFields';
import { SinkInfo } from '../common/SinkInfo';
import NodeSelect from '@/ui/components/NodeSelect';
import CreateTable from '@/ui/components/CreateTable';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, SyncCreateTableField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

const fieldTypesConf = {
  tinyint: (m, d) => (1 <= m && m <= 4 ? '' : '1<=M<=4'),
  smallint: (m, d) => (1 <= m && m <= 6 ? '' : '1<=M<=6'),
  mediumint: (m, d) => (1 <= m && m <= 9 ? '' : '1<=M<=9'),
  int: (m, d) => (1 <= m && m <= 11 ? '' : '1<=M<=11'),
  float: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  bigint: (m, d) => (1 <= m && m <= 20 ? '' : '1<=M<=20'),
  double: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  numeric: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  decimal: (m, d) =>
    1 <= m && m <= 255 && 1 <= d && d <= 30 && d <= m - 2 ? '' : '1<=M<=255,1<=D<=30,D<=M-2',
  boolean: () => '',
  date: () => '',
  time: () => '',
  datetime: () => '',
  char: (m, d) => (1 <= m && m <= 255 ? '' : '1<=M<=255'),
  varchar: (m, d) => (1 <= m && m <= 16383 ? '' : '1<=M<=16383'),
  text: () => '',
  binary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  varbinary: (m, d) => (1 <= m && m <= 64 ? '' : '1<=M<=64'),
  blob: () => '',
};

const fieldTypes = Object.keys(fieldTypesConf).reduce(
  (acc, key) =>
    acc.concat({
      label: key,
      value: key,
    }),
  [],
);

export default class HiveSink extends SinkInfo implements DataWithBackend, RenderRow, RenderList {
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.OceanBase.DatabaseName')
  @SyncField()
  @IngestionField()
  databaseName: string;

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
  @I18n('meta.Sinks.OceanBase.TableName')
  @SyncField()
  @IngestionField()
  tableName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
    }),
  })
  @ColumnDecorator()
  @I18n('meta.Sinks.OceanBase.PrimaryKey')
  @SyncField()
  @IngestionField()
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
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110].includes(values?.status),
      nodeType: 'OceanBase',
    }),
  })
  @SyncField()
  @IngestionField()
  @I18n('meta.Sinks.DataNodeName')
  dataNodeName: string;

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
      initialValue: fieldTypes[0].value,
      type: 'autocomplete',
      props: (text, record, idx, isNew) => ({
        options: fieldTypes,
        disabled: [110].includes(sinkValues?.status as number) && !isNew,
        allowClear: true,
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
      title: i18n.t('meta.Sinks.OceanBase.IsMetaField'),
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
      title: i18n.t('meta.Sinks.OceanBase.FieldFormat'),
      dataIndex: 'fieldFormat',
      initialValue: 0,
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
