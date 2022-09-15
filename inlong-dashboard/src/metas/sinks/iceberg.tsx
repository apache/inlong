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
import type { FieldItemType } from '@/metas/common';
import EditableTable from '@/components/EditableTable';
import { sourceFields } from './common/sourceFields';

const icebergFieldTypes = [
  'string',
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'decimal',
  'date',
  'time',
  'timestamp',
  'timestamptz',
  'binary',
  'fixed',
  'uuid',
].map(item => ({
  label: item,
  value: item,
}));

const matchPartitionStrategies = fieldType => {
  const data = [
    {
      label: 'None',
      value: 'None',
      disabled: false,
    },
    {
      label: 'Identity',
      value: 'Identity',
      disabled: false,
    },
    {
      label: 'Year',
      value: 'Year',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Month',
      value: 'Month',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Day',
      value: 'Day',
      disabled: !['timestamp', 'date'].includes(fieldType),
    },
    {
      label: 'Hour',
      value: 'Hour',
      disabled: fieldType !== 'timestamp',
    },
    {
      label: 'Bucket',
      value: 'Bucket',
      disabled: ![
        'string',
        'boolean',
        'short',
        'int',
        'long',
        'float',
        'double',
        'decimal',
      ].includes(fieldType),
    },
    {
      label: 'Truncate',
      value: 'Truncate',
      disabled: !['string', 'int', 'long', 'binary', 'decimal'].includes(fieldType),
    },
  ];

  return data.filter(item => !item.disabled);
};

export const iceberg: FieldItemType[] = [
  {
    name: 'dbName',
    type: 'input',
    label: i18n.t('meta.Sinks.Iceberg.DbName'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'tableName',
    type: 'input',
    label: i18n.t('meta.Sinks.Iceberg.TableName'),
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
    label: 'Catalog URI',
    name: 'catalogUri',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'thrift://127.0.0.1:9083',
    }),
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.Iceberg.Warehouse'),
    name: 'warehouse',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: 'hdfs://127.0.0.1:9000/user/iceberg/warehouse',
    }),
  },
  {
    name: 'fileFormat',
    type: 'radio',
    label: i18n.t('meta.Sinks.Iceberg.FileFormat'),
    initialValue: 'Parquet',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'Parquet',
          value: 'Parquet',
        },
        {
          label: 'Orc',
          value: 'Orc',
        },
        {
          label: 'Avro',
          value: 'Avro',
        },
      ],
    }),
  },
  {
    name: 'extList',
    label: i18n.t('meta.Sinks.Iceberg.ExtList'),
    type: EditableTable,
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
    initialValue: [],
  },
  {
    name: 'dataConsistency',
    type: 'select',
    label: i18n.t('meta.Sinks.Iceberg.DataConsistency'),
    initialValue: 'EXACTLY_ONCE',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'EXACTLY_ONCE',
          value: 'EXACTLY_ONCE',
        },
        {
          label: 'AT_LEAST_ONCE',
          value: 'AT_LEAST_ONCE',
        },
      ],
    }),
    isPro: true,
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
      title: `Iceberg ${i18n.t('meta.Sinks.Iceberg.FieldName')}`,
      width: 110,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z_][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.Iceberg.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `Iceberg ${i18n.t('meta.Sinks.Iceberg.FieldType')}`,
      dataIndex: 'fieldType',
      width: 130,
      initialValue: icebergFieldTypes[0].value,
      type: 'select',
      rules: [{ required: true }],
      props: (text, record, idx, isNew) => ({
        options: icebergFieldTypes,
        onChange: value => {
          const partitionStrategies = matchPartitionStrategies(value);
          if (partitionStrategies.every(item => item.value !== record.partitionStrategy)) {
            return {
              partitionStrategy: partitionStrategies[0].value,
            };
          }
        },
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: 'Length',
      dataIndex: 'fieldLength',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'fixed',
    },
    {
      title: 'Precision',
      dataIndex: 'fieldPrecision',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'decimal',
    },
    {
      title: 'Scale',
      dataIndex: 'fieldScale',
      type: 'inputnumber',
      props: {
        min: 0,
      },
      initialValue: 1,
      rules: [{ type: 'number', required: true }],
      visible: (text, record) => record.fieldType === 'decimal',
    },
    {
      title: i18n.t('meta.Sinks.Iceberg.PartitionStrategy'),
      dataIndex: 'partitionStrategy',
      type: 'select',
      initialValue: 'None',
      rules: [{ required: true }],
      props: (text, record) => ({
        options: matchPartitionStrategies(record.fieldType),
      }),
    },
    {
      title: i18n.t('meta.Sinks.Iceberg.FieldDescription'),
      dataIndex: 'fieldComment',
    },
  ];
};
