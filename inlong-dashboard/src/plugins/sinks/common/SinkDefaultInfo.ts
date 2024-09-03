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
import i18n from '@/i18n';
import CheckCard from '@/ui/components/CheckCard';
import { statusList, genStatusTag } from './status';
import { sinks, defaultValue } from '..';

const { I18nMap, I18n } = DataWithBackend;
const {
  FieldList,
  FieldDecorator,
  SyncField,
  SyncFieldSet,
  SyncMoveDbField,
  SyncMoveDbFieldSet,
  SyncCreateTableFieldSet,
  IngestionField,
  IngestionFieldSet,
} = RenderRow;
const { ColumnList, ColumnDecorator } = RenderList;

export class SinkDefaultInfo implements DataWithBackend, RenderRow, RenderList {
  static I18nMap = I18nMap;
  static FieldList = FieldList;
  static ColumnList = ColumnList;
  static SyncFieldSet = SyncFieldSet;
  static SyncMoveDbFieldSet = SyncMoveDbFieldSet;
  static SyncCreateTableFieldSet = SyncCreateTableFieldSet;
  static IngestionFieldSet = IngestionFieldSet;

  readonly id: number;

  @FieldDecorator({
    // This field is not visible or editable, but form value should exists.
    type: 'text',
    hidden: true,
  })
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('inlongGroupId')
  readonly inlongGroupId: string;

  @FieldDecorator({
    type: 'text',
    hidden: true,
  })
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('inlongStreamId')
  readonly inlongStreamId: string;

  @FieldDecorator({
    type: CheckCard,
    label: i18n.t('meta.Sinks.SinkType'),
    rules: [{ required: true }],
    initialValue: defaultValue,
    props: values => ({
      span: 4,
      dropdownMatchSelectWidth: false,
      disabled: !!values.id,
      options: sinks
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
          image: loadImage(item.label),
        })),
    }),
  })
  @ColumnDecorator({
    render: type => sinks.find(c => c.value === type)?.label || type,
  })
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('meta.Sinks.SinkType')
  sinkType: string;

  @FieldDecorator({
    type: 'input',
    rules: [
      { required: true },
      {
        pattern: /^[a-zA-Z0-9_.-]*$/,
        message: i18n.t('meta.Sinks.SinkNameRule'),
      },
    ],
    props: values => ({
      disabled: !!values.id,
      maxLength: 100,
    }),
    visible: values => Boolean(values.sinkType),
  })
  @ColumnDecorator()
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('meta.Sinks.SinkName')
  sinkName: string;

  @FieldDecorator({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 300,
    },
    visible: values => Boolean(values.sinkType),
  })
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('meta.Sinks.Description')
  description: string;

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
    visible: values => Boolean(values.sinkType),
  })
  @SyncField()
  @SyncMoveDbField()
  @IngestionField()
  @I18n('meta.Sinks.EnableCreateResource')
  enableCreateResource: number;

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
  @SyncMoveDbField()
  @IngestionField()
  @I18n('basic.Status')
  readonly status: string;

  @ColumnDecorator()
  @I18n('basic.Creator')
  @IngestionField()
  readonly creator: string;

  @ColumnDecorator()
  @IngestionField()
  @I18n('basic.Modifier')
  readonly modifier: string;

  parse(data) {
    return data;
  }

  stringify(data) {
    const { sinkType } = data;

    if (Array.isArray(data.sinkFieldList)) {
      data.sinkFieldList = data.sinkFieldList.map(item => ({
        ...item,
        sinkType,
      }));
    }

    return data;
  }

  renderSyncRow() {
    const constructor = this.constructor as typeof SinkDefaultInfo;
    const { FieldList, SyncFieldSet } = constructor;
    return FieldList.filter(item => SyncFieldSet.has(item.name as string));
  }

  renderSyncCreateTableRow() {
    const constructor = this.constructor as typeof SinkDefaultInfo;
    const { FieldList, SyncCreateTableFieldSet } = constructor;
    return FieldList.filter(item => SyncCreateTableFieldSet.has(item.name as string));
  }

  renderSyncAllRow() {
    const constructor = this.constructor as typeof SinkDefaultInfo;
    const { FieldList, SyncMoveDbFieldSet } = constructor;
    return FieldList.filter(item => {
      if (item.name === 'sinkType') {
        item.props = values => ({
          disabled: Boolean(values.id),
          dropdownMatchSelectWidth: false,
          options: sinks
            .filter(item => item.value === 'ICEBERG' || item.value === 'DORIS')
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
    const constructor = this.constructor as typeof SinkDefaultInfo;
    const { FieldList, IngestionFieldSet } = constructor;
    return FieldList.filter(item => {
      if (item.name === 'tableName' || item.name === 'primaryKey' || item.name === 'database') {
        item.type = 'input';
      }
      return IngestionFieldSet.has(item.name as string);
    });
  }

  renderList() {
    const constructor = this.constructor as typeof SinkDefaultInfo;
    return constructor.ColumnList;
  }
}
