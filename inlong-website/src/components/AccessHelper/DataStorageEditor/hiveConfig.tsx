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

import React from 'react';
import { Button, message } from 'antd';
import EditableTable, { ColumnsItemProps } from '@/components/EditableTable';
import request from '@/utils/request';
import { fieldTypes } from '../FieldsConfig/sourceFields';

// hiveFieldTypes
const hiveFieldTypes = [
  'tinyint',
  'smallint',
  'int',
  'bigint',
  'float',
  'double',
  // 'decimal',
  // 'numeric',
  // 'timestamp',
  // 'date',
  'string',
  // 'varchar',
  // 'char',
  'boolean',
  'binary',
].map(item => ({
  label: item,
  value: item,
}));

export const hiveTableColumns = [
  {
    title: '目标库',
    dataIndex: 'dbName',
  },
  {
    title: '目标表',
    dataIndex: 'tableName',
  },
  {
    title: '用户名',
    dataIndex: 'username',
  },
  {
    title: 'JDBC URL',
    dataIndex: 'jdbcUrl',
  },
  {
    title: 'HDFS DefaultFS',
    dataIndex: 'hdfsDefaultFs',
  },
];

export const getHiveColumns = (dataType: string, storageType: string): ColumnsItemProps[] => [
  ...([
    {
      title: '源字段名',
      dataIndex: 'sourceFieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: '以英文字母开头，只能包含英文字母、数字、下划线',
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: text && !isNew,
      }),
    },
    {
      title: '源字段类型',
      dataIndex: 'sourceFieldType',
      initialValue: fieldTypes[0].value,
      type: 'select',
      rules: [{ required: true }],
      props: (text, record, idx, isNew) => ({
        disabled: text && !isNew,
        options: fieldTypes,
      }),
    },
    {
      title: `${storageType}字段名`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: '以英文字母开头，只能包含英文字母、数字、下划线',
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: text && !isNew,
      }),
    },
    {
      title: `${storageType}字段类型`,
      dataIndex: 'fieldType',
      initialValue: hiveFieldTypes[0].value,
      type: 'select',
      props: () => ({
        options: hiveFieldTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: '字段描述',
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ] as ColumnsItemProps[]),
];

export const getHiveForm = (dataType: string, isEdit: boolean, form) => [
  {
    type: 'input',
    label: '目标库',
    name: 'dbName',
    rules: [{ required: true }],
    props: {
      disabled: isEdit,
    },
  },
  {
    type: 'input',
    label: '目标表',
    name: 'tableName',
    rules: [{ required: true }],
    props: {
      disabled: isEdit,
    },
  },
  {
    type: 'input',
    name: 'primaryPartition',
    label: '一级分区',
    initialValue: 'dt',
    rules: [{ required: true }],
    props: {
      disabled: isEdit,
    },
  },
  {
    type: 'input',
    label: '用户名',
    name: 'username',
    rules: [{ required: true }],
    props: {
      disabled: isEdit,
    },
  },
  {
    type: 'password',
    label: '用户密码',
    name: 'password',
    rules: [{ required: true }],
    props: {
      disabled: isEdit,
      style: {
        maxWidth: 500,
      },
    },
  },
  {
    type: 'input',
    label: 'JDBC URL',
    name: 'jdbcUrl',
    rules: [{ required: true }],
    props: {
      placeholder: 'jdbc:hive2://127.0.0.1:10000',
      disabled: isEdit,
      style: { width: 500 },
    },
    suffix: (
      <Button
        onClick={async () => {
          const values = await form.validateFields(['username', 'password', 'jdbcUrl']);
          const res = await request({
            url: '/storage/query/testConnection',
            method: 'POST',
            data: values,
          });
          res ? message.success('连接成功') : message.error('连接失败');
        }}
      >
        连接测试
      </Button>
    ),
  },
  {
    type: 'input',
    label: 'HDFS DefaultFS',
    name: 'hdfsDefaultFs',
    rules: [{ required: true }],
    props: {
      placeholder: 'hdfs://127.0.0.1:9000',
      disabled: isEdit,
    },
  },
  {
    type: 'input',
    label: 'Warehouse路径',
    name: 'warehouseDir',
    initialValue: '/user/hive/warehouse',
    props: {
      disabled: isEdit,
    },
  },
  {
    type: (
      <EditableTable
        size="small"
        columns={getHiveColumns(dataType, 'HIVE')}
        canDelete={(record, idx, isNew) => !isEdit || isNew}
      />
    ),
    name: 'hiveFieldList',
  },
];
