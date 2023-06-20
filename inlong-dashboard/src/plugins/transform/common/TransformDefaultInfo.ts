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
import CheckCard from '@/ui/components/CheckCard';
import { statusList, genStatusTag } from './status';
import { transform, defaultValue } from '..';

const { I18nMap, I18n } = DataWithBackend;
const { FieldList, FieldDecorator } = RenderRow;
const { ColumnList, ColumnDecorator } = RenderList;

export class TransformDefaultInfo implements DataWithBackend, RenderRow, RenderList {
  static I18nMap = I18nMap;
  static FieldList = FieldList;
  static ColumnList = ColumnList;

  readonly id: number;

  @FieldDecorator({
    // This field is not visible or editable, but form value should exists.
    type: 'text',
    hidden: true,
  })
  @I18n('inlongGroupId')
  readonly inlongGroupId: string;

  @FieldDecorator({
    type: 'text',
    hidden: true,
  })
  @I18n('inlongStreamId')
  readonly inlongStreamId: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
      maxLength: 128,
    }),
    // visible: values => Boolean(values.sourceType),
  })
  @ColumnDecorator()
  @I18n('转换节点名称')
  transformName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    initialValue: defaultValue,
    props: values => ({
      disabled: Boolean(values.id),
      dropdownMatchSelectWidth: false,
      options: transform
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
        })),
    }),
  })
  @ColumnDecorator()
  @I18n('转换类型')
  // sourceType: string;
  transformType: string;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }

  renderRow() {
    const constructor = this.constructor as typeof TransformDefaultInfo;
    return constructor.FieldList;
  }

  renderList() {
    const constructor = this.constructor as typeof TransformDefaultInfo;
    return constructor.ColumnList;
  }
}
