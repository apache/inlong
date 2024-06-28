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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import i18n from '@/i18n';
import EditableTable from '@/ui/components/EditableTable';
import { fieldTypes as sourceFieldsTypes } from '@/plugins/sinks/common/sourceFields';
import UserSelect from '@/ui/components/UserSelect';

const { I18nMap, I18n } = DataWithBackend;
const { FieldList, FieldDecorator } = RenderRow;
const { ColumnList, ColumnDecorator } = RenderList;

export class GroupDataTemplateInfo implements DataWithBackend, RenderRow, RenderList {
  static I18nMap = I18nMap;
  static FieldList = FieldList;
  static ColumnList = ColumnList;

  readonly id: number;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
  })
  @ColumnDecorator()
  @I18n('pages.GroupDataTemplate.Name')
  name: string;

  @FieldDecorator({
    type: UserSelect,
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  })
  @ColumnDecorator()
  @I18n('pages.GroupDataTemplate.InCharges')
  inCharges: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'ALL',
    props: values => ({
      options: [
        {
          label: i18n.t('pages.GroupDataTemplate.VisibleRange.All'),
          value: 'ALL',
        },
        {
          label: i18n.t('pages.GroupDataTemplate.VisibleRange.InCharges'),
          value: 'IN_CHARGE',
        },
        {
          label: i18n.t('pages.GroupDataTemplate.VisibleRange.Tenant'),
          value: 'TENANT',
        },
      ],
    }),
    rules: [{ required: true }],
  })
  @I18n('pages.GroupDataTemplate.VisibleRange')
  visibleRange: String;

  @FieldDecorator({
    type: 'select',
    hidden: true,
    props: {
      mode: 'multiple',
      filterOption: true,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/tenant/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 9999,
          },
        }),
        requestParams: {
          formatResult: result => {
            return result?.list?.map(item => ({
              ...item,
              label: item.name,
              value: item.name.toString(),
            }));
          },
        },
      },
    },
    rules: [
      {
        required: true,
      },
    ],
  })
  @ColumnDecorator()
  @I18n('pages.GroupDataTemplate.TenantList')
  tenantList: string[];

  @FieldDecorator({
    type: 'input',
    initialValue: 0,
    hidden: true,
    rules: [{ required: true }],
  })
  @I18n('pages.GroupDataTemplate.Version')
  version: number;

  @FieldDecorator({
    type: EditableTable,
    props: values => ({
      size: 'small',
      canDelete: record => !(record.id && [110].includes(values?.status)),
      canBatchAdd: true,
      columns: [
        {
          title: i18n.t('meta.Stream.FieldName'),
          dataIndex: 'fieldName',
          rules: [
            { required: true },
            {
              pattern: /^[_a-zA-Z][a-zA-Z0-9_]*$/,
              message: i18n.t('meta.Stream.FieldNameRule'),
            },
          ],
          props: (text, record) => ({
            disabled: record.id && [110].includes(values?.status),
          }),
        },
        {
          title: i18n.t('meta.Stream.FieldType'),
          dataIndex: 'fieldType',
          type: 'select',
          initialValue: sourceFieldsTypes[0].value,
          props: (text, record) => ({
            disabled: record.id && [110].includes(values?.status),
            options: sourceFieldsTypes,
          }),
          rules: [{ required: true }],
        },
        {
          title: i18n.t('meta.Stream.FieldComment'),
          dataIndex: 'fieldComment',
        },
      ],
    }),
  })
  @I18n('meta.Stream.SourceDataField')
  fieldList: Record<string, string>[];

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }

  renderRow() {
    const constructor = this.constructor as typeof GroupDataTemplateInfo;
    return constructor.FieldList;
  }

  renderList() {
    const constructor = this.constructor as typeof GroupDataTemplateInfo;
    return constructor.ColumnList;
  }
}
