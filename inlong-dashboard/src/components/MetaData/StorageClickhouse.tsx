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

import {
  getColsFromFields,
  GetStorageColumnsType,
  GetStorageFormFieldsType,
} from '@/utils/metaData';
import i18n from '@/i18n';
import { ColumnsType } from 'antd/es/table';
import EditableTable from '@/components/EditableTable';
import { excludeObject } from '@/utils';
import { sourceDataFields } from './SourceDataFields';

// CLICK_HOUSE targetType
const clickhouseTargetTypes = [
  'string',
  'boolean',
  'byte',
  'short',
  'int',
  'long',
  'float',
  'double',
  'decimal',
  // 'time',
  // 'date',
  // 'timestamp',
  // 'map<string,string>',
  // 'map<string,map<string,string>>',
  // 'map<string,map<string,map<string,string>>>',
].map(item => ({
  label: item,
  value: item,
}));

// Auto map to source fieldType
export const fieldTypeMap = {
  int: 'int',
  long: 'long',
  float: 'float',
  double: 'double',
  string: 'string',
  date: 'string',
  timestamp: 'string',
};

export const getClickhouseForm: GetStorageFormFieldsType = (
  type: 'form' | 'col' = 'form',
  { currentValues, inlongGroupId, isEdit, dataType } = {} as any,
) => {
  const fileds = [
    {
      name: 'clusterId',
      type: 'select',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Cluster'),
      rules: [{ required: true }],
      props: {
        options: {
          requestService: {
            url: '/storage/listStorageCluster',
            params: {
              storageType: 'CLICK_HOUSE',
            },
          },
          requestParams: {
            formatResult: result =>
              result.clickHouseClusterList?.map(item => ({
                label: item.name,
                value: item.id,
              })),
          },
          requestAuto: isEdit,
        },
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
    },
    {
      name: 'dbName',
      type: 'input',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Db'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _col: true,
    },
    {
      name: 'tableName',
      type: 'input',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Table'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _col: true,
    },
    {
      name: 'flushInterval',
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.FlushInterval'),
      initialValue: 1,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.FlushIntervalUnit'),
    },
    {
      name: 'packageSize',
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.PackageSize'),
      initialValue: 1000,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.PackageSizeUnit'),
    },
    {
      name: 'retryTime',
      type: 'inputnumber',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.RetryTime'),
      initialValue: 3,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.RetryTimeUnit'),
    },
    {
      name: 'username',
      type: 'input',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Username'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
    },
    {
      name: 'password',
      type: 'input',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Password'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
    },
    {
      name: 'isDistribute',
      type: 'radio',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.IsDistribute'),
      initialValue: 0,
      props: {
        options: [
          {
            label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.Yes'),
            value: 1,
          },
          {
            label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.No'),
            value: 0,
          },
        ],
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
    },
    {
      name: 'partitionStrategy',
      type: 'select',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.PartitionStrategy'),
      initialValue: 'BALANCE',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: 'BALANCE',
            value: 'BALANCE',
          },
          {
            label: 'RANDOM',
            value: 'RANDOM',
          },
          {
            label: 'HASH',
            value: 'HASH',
          },
        ],
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      visible: values => values.isDistribute,
      _col: true,
    },
    {
      name: 'partitionFields',
      type: 'input',
      label: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.PartitionFields'),
      rules: [{ required: true }],
      visible: values => values.isDistribute && values.partitionStrategy === 'HASH',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
    },
    {
      name: 'fieldList',
      type: EditableTable,
      props: {
        size: 'small',
        editing: ![110, 130].includes(currentValues?.status),
        columns: getClickhouseColumns(dataType, currentValues),
      },
    },
  ];

  return type === 'col'
    ? getColsFromFields(fileds)
    : fileds.map(item => excludeObject(['_col'], item));
};

export const getClickhouseColumns: GetStorageColumnsType = (dataType, currentValues) => {
  return [
    ...sourceDataFields,
    {
      title: `ClickHouse${i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.FieldName')}`,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('components.AccessHelper.StorageMetaData.Clickhouse.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: clickhouseTargetTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
        options: clickhouseTargetTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: `ClickHouse${i18n.t(
        'components.AccessHelper.StorageMetaData.Clickhouse.FieldDescription',
      )}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
    },
  ];
};

export const clickhouseTableColumns = getClickhouseForm('col') as ColumnsType;
