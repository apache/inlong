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
import i18n from '@/i18n';
import EditableTable from '@/components/EditableTable';
import { fieldTypes as sourceFieldsTypes } from '@/metas/sinks/common/sourceFields';
import { statusList, genStatusTag } from './status';

const { I18n, FormField, TableColumn } = DataWithBackend;

export class StreamDefaultInfo extends DataWithBackend {
  readonly id: number;

  @FormField({
    type: 'input',
    props: {
      maxLength: 32,
    },
    rules: [
      { required: true },
      {
        pattern: /^[0-9a-z_-]+$/,
        message: i18n.t('meta.Stream.StreamIdRules'),
      },
    ],
  })
  @TableColumn()
  @I18n('meta.Stream.StreamId')
  inlongStreamId: string;

  @FormField({
    type: 'input',
  })
  @TableColumn()
  @I18n('meta.Stream.StreamName')
  name: string;

  @FormField({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 256,
    },
  })
  @I18n('meta.Stream.Description')
  description: string;

  @TableColumn()
  @I18n('basic.Creator')
  readonly creator: string;

  @TableColumn()
  @I18n('basic.CreateTime')
  readonly createTime: string;

  @FormField({
    type: 'select',
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
    visible: false,
  })
  @TableColumn({
    render: text => genStatusTag(text),
  })
  @I18n('basic.Status')
  status: string;

  @FormField({
    type: 'radio',
    initialValue: 'CSV',
    tooltip: i18n.t('meta.Stream.DataTypeCsvHelp'),
    props: {
      options: [
        {
          label: 'CSV',
          value: 'CSV',
        },
        {
          label: 'KEY-VALUE',
          value: 'KEY-VALUE',
        },
        {
          label: 'JSON',
          value: 'JSON',
        },
        {
          label: 'AVRO',
          value: 'AVRO',
        },
      ],
    },
    rules: [{ required: true }],
  })
  @I18n('meta.Stream.DataType')
  dataType: string;

  @FormField({
    type: 'radio',
    initialValue: 'UTF-8',
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
  })
  @I18n('meta.Stream.DataEncoding')
  dataEncoding: string;

  @FormField({
    type: 'select',
    initialValue: '124',
    props: {
      dropdownMatchSelectWidth: false,
      options: [
        {
          label: i18n.t('meta.Stream.DataSeparator.Space'),
          value: '32',
        },
        {
          label: i18n.t('meta.Stream.DataSeparator.VerticalLine'),
          value: '124',
        },
        {
          label: i18n.t('meta.Stream.DataSeparator.Comma'),
          value: '44',
        },
        {
          label: i18n.t('meta.Stream.DataSeparator.Semicolon'),
          value: '59',
        },
        {
          label: i18n.t('meta.Stream.DataSeparator.Asterisk'),
          value: '42',
        },
        {
          label: i18n.t('meta.Stream.DataSeparator.DoubleQuotes'),
          value: '34',
        },
      ],
      useInput: true,
      useInputProps: {
        placeholder: 'ASCII',
      },
      style: { width: 100 },
    },
    rules: [
      {
        required: true,
        type: 'number',
        transform: val => +val,
        min: 0,
        max: 127,
      },
    ],
  })
  @I18n('meta.Stream.DataSeparator')
  dataSeparator: string;

  @FormField({
    type: EditableTable,
    props: {
      size: 'small',
      columns: [
        {
          title: i18n.t('meta.Stream.FieldName'),
          dataIndex: 'fieldName',
          rules: [
            { required: true },
            {
              pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
              message: i18n.t('meta.Stream.FieldNameRule'),
            },
          ],
        },
        {
          title: i18n.t('meta.Stream.FieldType'),
          dataIndex: 'fieldType',
          type: 'select',
          initialValue: sourceFieldsTypes[0].value,
          props: {
            options: sourceFieldsTypes,
          },
          rules: [{ required: true }],
        },
        {
          title: i18n.t('meta.Stream.FieldComment'),
          dataIndex: 'fieldComment',
        },
      ],
    },
  })
  @I18n('meta.Stream.SourceDataField')
  rowTypeFields: Record<string, string>[];

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }
}
