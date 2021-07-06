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
import { FormItemProps } from '@/components/FormGenerator';
import { pickObjectArray } from '@/utils';
import StaffSelect from '@/components/StaffSelect';
import DataSourcesEditor from '../DataSourcesEditor';
import DataStorageEditor from '../DataStorageEditor/Editor';
import EditableTable from '@/components/EditableTable';
import { fieldTypes as sourceFieldsTypes } from './sourceFields';

type RestParams = {
  businessIdentifier?: string;
  // Whether to use true operation for data source management
  useDataSourcesActionRequest?: boolean;
  // Whether to use real operation for data storage management
  useDataStorageActionRequest?: boolean;
  // Whether the fieldList is in edit mode
  fieldListEditing?: boolean;
  readonly?: boolean;
};

export default (
  names: string[],
  currentValues: Record<string, any> = {},
  {
    businessIdentifier: bid,
    useDataSourcesActionRequest = false,
    useDataStorageActionRequest = false,
    fieldListEditing = true,
    readonly = false,
  }: RestParams = {},
): FormItemProps[] => {
  const basicProps = {
    businessIdentifier: bid,
    dataStreamIdentifier: currentValues.dataStreamIdentifier,
  };

  const fields: FormItemProps[] = [
    {
      type: 'input',
      label: '数据流ID',
      name: 'dataStreamIdentifier',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.dataStreamIdentifier,
      rules: [
        { required: true },
        { pattern: /^[a-z_\d]+$/, message: '仅限小写字⺟、数字和下划线' },
      ],
    },
    {
      type: 'input',
      label: '数据流名称',
      name: 'name',
      initialValue: currentValues.name,
      rules: [{ required: true }],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: '数据流责任人',
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      extra: '至少2人，责任人可查看、修改消费信息',
      rules: [{ required: true, type: 'array', min: 2, message: '请填写至少2个责任人' }],
    },
    {
      type: 'textarea',
      label: '数据流介绍',
      name: 'description',
      props: {
        showCount: true,
        maxLength: 100,
      },
      initialValue: currentValues.desc,
      rules: [{ required: true }],
    },
    {
      type: 'radio',
      label: '消息来源',
      name: 'dataSourceType',
      initialValue: currentValues.dataSourceType ?? 'AUTO_PUSH',
      props: {
        options: [
          {
            label: '文件',
            value: 'FILE',
          },
          {
            label: '自主推送',
            value: 'AUTO_PUSH',
          },
        ],
      },
      rules: [{ required: true }],
    },
    {
      type: (
        <DataSourcesEditor
          readonly={readonly}
          type={currentValues.dataSourceType}
          useActionRequest={useDataSourcesActionRequest}
          {...basicProps}
        />
      ),
      name: 'dataSourcesConfig',
      visible: values => values.dataSourceType === 'FILE',
    },
    {
      type: 'radio',
      label: '数据格式',
      name: 'dataType',
      initialValue: currentValues.dataType ?? 'TEXT',
      props: {
        options: [
          {
            label: 'TEXT',
            value: 'TEXT',
          },
          {
            label: 'KEY-VALUE',
            value: 'KEY-VALUE',
          },
        ],
      },
      rules: [{ required: true }],
    },
    {
      type: 'radio',
      label: '数据编码',
      name: 'dataEncoding',
      initialValue: currentValues.dataEncoding ?? 'UTF-8',
      props: {
        options: [
          {
            label: 'UTF-8',
            value: 'UTF-8',
          },
          {
            label: 'GBK',
            value: 'GBK',
          },
        ],
      },
      rules: [{ required: true }],
      visible: values => values.dataSourceType === 'FILE' || values.dataSourceType === 'AUTO_PUSH',
    },
    {
      type: 'select',
      label: '源数据字段分割符',
      name: 'fileDelimiter',
      initialValue: '9',
      props: {
        dropdownMatchSelectWidth: false,
        options: [
          {
            label: 'TAB键',
            value: '9',
          },
          {
            label: '竖线(|)',
            value: '124',
          },
          {
            label: '逗号',
            value: '44',
          },
          {
            label: '双引号(")',
            value: '34',
          },
          {
            label: '星号(*)',
            value: '42',
          },
          {
            label: '空格',
            value: '32',
          },
          {
            label: '分号(;)',
            value: '59',
          },
        ],
      },
      rules: [{ required: true }],
      suffix: {
        type: 'input',
        name: 'fileDelimiterDetail',
        rules: [
          {
            required: true,
          },
        ],
        visible: values => values.fileDelimiter === 'ASCII',
      },
      visible: values => values.dataSourceType === 'FILE' || values.dataSourceType === 'AUTO_PUSH',
    },
    {
      type: (
        <EditableTable
          size="small"
          editing={fieldListEditing}
          columns={[
            {
              title: '字段名',
              dataIndex: 'fieldName',
              props: () => ({
                disabled: !fieldListEditing,
              }),
              rules: [
                { required: true },
                {
                  pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
                  message: '以英文字母开头，只能包含英文字母、数字、下划线',
                },
              ],
            },
            {
              title: '字段类型',
              dataIndex: 'fieldType',
              type: 'select',
              initialValue: sourceFieldsTypes[0].value,
              props: () => ({
                disabled: !fieldListEditing,
                options: sourceFieldsTypes,
              }),
              rules: [{ required: true }],
            },
            {
              title: '字段描述',
              dataIndex: 'fieldComment',
            },
          ]}
        />
      ),
      label: '源数据字段',
      name: 'rowTypeFields',
      visible: () => !(currentValues.dataType as string[])?.includes('PB'),
    },
    {
      type: 'checkboxgroup',
      label: '数据流向',
      name: 'dataStorage',
      props: {
        options: [
          {
            label: 'HIVE',
            value: 'HIVE',
          },
          {
            label: '自主消费',
            value: 'AUTO_PUSH',
          },
        ],
      },
    },
    ...['HIVE'].reduce(
      (acc, item) =>
        acc.concat({
          type: (
            <DataStorageEditor
              readonly={readonly}
              defaultRowTypeFields={currentValues.rowTypeFields}
              type={item}
              dataType={currentValues.dataType}
              useActionRequest={useDataStorageActionRequest}
              {...basicProps}
            />
          ),
          name: `dataStorage${item}`,
          visible: values => (values.dataStorage as string[])?.includes(item),
        }),
      [],
    ),
  ];

  return pickObjectArray(names, fields);
};
