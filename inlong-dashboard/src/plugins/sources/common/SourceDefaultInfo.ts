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
import { loadImage } from '@/plugins/images';
import CheckCard from '@/ui/components/CheckCard';
import { statusList, genStatusTag } from './status';
import { sources, defaultValue } from '..';
import i18n from '@/i18n';
import dayjs from 'dayjs';

const { I18nMap, I18n } = DataWithBackend;
const {
  FieldList,
  FieldDecorator,
  SyncField,
  SyncFieldSet,
  SyncMoveDbField,
  SyncMoveDbFieldSet,
  IngestionField,
  IngestionFieldSet,
} = RenderRow;
const { ColumnList, ColumnDecorator } = RenderList;

export class SourceDefaultInfo implements DataWithBackend, RenderRow, RenderList {
  static I18nMap = I18nMap;
  static FieldList = FieldList;
  static ColumnList = ColumnList;
  static SyncFieldSet = SyncFieldSet;
  static SyncMoveDbFieldSet = SyncMoveDbFieldSet;
  static IngestionFieldSet = IngestionFieldSet;

  readonly id: number;

  @FieldDecorator({
    // This field is not visible or editable, but form value should exists.
    type: 'text',
    hidden: true,
  })
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  @I18n('inlongGroupId')
  readonly inlongGroupId: string;

  @FieldDecorator({
    type: 'text',
    hidden: true,
  })
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  @I18n('inlongStreamId')
  readonly inlongStreamId: string;

  @FieldDecorator({
    type: CheckCard,
    rules: [{ required: true }],
    initialValue: defaultValue,
    props: values => ({
      disabled: Boolean(values.id),
      dropdownMatchSelectWidth: false,
      options: sources
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
          image: loadImage(item.label),
        })),
    }),
  })
  @ColumnDecorator({
    render: type => sources.find(c => c.value === type)?.label || type,
  })
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  @I18n('meta.Sources.Type')
  sourceType: string;

  @FieldDecorator({
    type: 'select',
    hidden: true,
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.ClusterName')
  clusterTag: string;

  @FieldDecorator({
    type: 'input',
    rules: [
      { required: true },
      {
        pattern: /^[a-zA-Z0-9_.-]*$/,
        message: i18n.t('meta.Sources.NameRule'),
      },
    ],
    props: values => ({
      disabled: Boolean(values.id),
      maxLength: 100,
    }),
    visible: values => Boolean(values.sourceType),
  })
  @ColumnDecorator()
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  @I18n('meta.Sources.Name')
  sourceName: string;

  @ColumnDecorator()
  @I18n('meta.Sources.ClusterName')
  readonly inlongClusterName: string;

  @FieldDecorator({
    type: 'select',
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
    visible: false,
  })
  @ColumnDecorator({
    render: text => genStatusTag(text),
  })
  @SyncField()
  @IngestionField()
  @SyncMoveDbField()
  @I18n('basic.Status')
  readonly status: string;

  @ColumnDecorator()
  @IngestionField()
  @I18n('basic.Creator')
  readonly creator: string;

  @ColumnDecorator()
  @IngestionField()
  @I18n('basic.Modifier')
  readonly modifier: string;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }

  renderSyncRow() {
    const constructor = this.constructor as typeof SourceDefaultInfo;
    const { FieldList, SyncFieldSet } = constructor;
    return FieldList.filter(item => {
      if (item.name === 'sourceType') {
        item.props = values => ({
          disabled: Boolean(values.id),
          dropdownMatchSelectWidth: false,
          options: sources
            .filter(item => item.useSync !== false)
            .map(item => ({
              label: item.label,
              value: item.value,
              image: loadImage(item.label),
            })),
        });
      }
      return SyncFieldSet.has(item.name as string);
    });
  }

  renderSyncEnableRow() {
    const constructor = this.constructor as typeof SourceDefaultInfo;
    const { FieldList, SyncMoveDbFieldSet } = constructor;
    return FieldList.filter(item => {
      if (item.name === 'sourceType') {
        item.props = values => ({
          disabled: Boolean(values.id),
          dropdownMatchSelectWidth: false,
          options: sources
            .filter(item => item.value === 'MYSQL_BINLOG')
            .map(item => ({
              label: item.label,
              value: item.value,
              image: loadImage(item.label),
            })),
        });
      }
      return SyncMoveDbFieldSet.has(item.name as string);
    });
  }

  renderRow() {
    const constructor = this.constructor as typeof SourceDefaultInfo;
    const { FieldList, IngestionFieldSet } = constructor;
    return FieldList.filter(item => {
      if (item.name === 'sourceType') {
        item.props = values => ({
          disabled: Boolean(values.id),
          dropdownMatchSelectWidth: false,
          options: sources
            .filter(item => item.value)
            .map(item => ({
              label: item.label,
              value: item.value,
              image: loadImage(item.label),
            })),
        });
      }
      return IngestionFieldSet.has(item.name as string);
    });
    // return constructor.FieldList;
  }

  renderList() {
    const constructor = this.constructor as typeof SourceDefaultInfo;
    return constructor.ColumnList;
  }
}
