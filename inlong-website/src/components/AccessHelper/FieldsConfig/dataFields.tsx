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
import i18n from '@/i18n';
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
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataStreamID'),
      name: 'dataStreamIdentifier',
      props: {
        maxLength: 32,
      },
      initialValue: currentValues.dataStreamIdentifier,
      rules: [
        { required: true },
        {
          pattern: /^[a-z_\d]+$/,
          message: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataStreamRules'),
        },
      ],
    },
    {
      type: 'input',
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataStreamName'),
      name: 'name',
      initialValue: currentValues.name,
      rules: [{ required: true }],
    },
    {
      type: <StaffSelect mode="multiple" currentUserClosable={false} />,
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataStreamOwners'),
      name: 'inCharges',
      initialValue: currentValues.inCharges,
      extra: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataStreamOwnerHelp'),
      rules: [
        {
          required: true,
        },
      ],
    },
    {
      type: 'textarea',
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataFlowIntroduction'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Source'),
      name: 'dataSourceType',
      initialValue: currentValues.dataSourceType ?? 'AUTO_PUSH',
      props: {
        options: [
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.File'),
            value: 'FILE',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Autonomous'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataType'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataEncoding'),
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
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.fileDelimiter'),
      name: 'fileDelimiter',
      initialValue: '9',
      props: {
        dropdownMatchSelectWidth: false,
        options: [
          {
            label: 'TAB',
            value: '9',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.VerticalLine'),
            value: '124',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Comma'),
            value: '44',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DoubleQuotes'),
            value: '34',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Asterisk'),
            value: '42',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Space'),
            value: '32',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.Semicolon'),
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
              title: i18n.t('components.AccessHelper.FieldsConfig.dataFields.FieldName'),
              dataIndex: 'fieldName',
              props: () => ({
                disabled: !fieldListEditing,
              }),
              rules: [
                { required: true },
                {
                  pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
                  message: i18n.t('components.AccessHelper.FieldsConfig.dataFields.FieldNameRule'),
                },
              ],
            },
            {
              title: i18n.t('components.AccessHelper.FieldsConfig.dataFields.FieldType'),
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
              title: i18n.t('components.AccessHelper.FieldsConfig.dataFields.FieldComment'),
              dataIndex: 'fieldComment',
            },
          ]}
        />
      ),
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.SourceDataField'),
      name: 'rowTypeFields',
      visible: () => !(currentValues.dataType as string[])?.includes('PB'),
    },
    {
      type: 'checkboxgroup',
      label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.DataFlowDirection'),
      name: 'dataStorage',
      props: {
        options: [
          {
            label: 'HIVE',
            value: 'HIVE',
          },
          {
            label: i18n.t('components.AccessHelper.FieldsConfig.dataFields.AutoConsumption'),
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
