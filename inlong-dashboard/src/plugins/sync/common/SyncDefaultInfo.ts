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
import UserSelect from '@/ui/components/UserSelect';
import { genStatusTag, statusList } from './status';
import { timestampFormat } from '@/core/utils';
import { DatePicker, TimePicker } from 'antd';
import dayjs from 'dayjs';
import { inlongGroupModeList } from '@/plugins/sync/common/SyncType';
import { range } from 'lodash';

const { I18nMap, I18n } = DataWithBackend;
const { FieldList, FieldDecorator } = RenderRow;
const { ColumnList, ColumnDecorator } = RenderList;

const format = 'HH:mm';
const conventionalTimeFormat = 'YYYY-MM-DD HH:mm';

export class SyncDefaultInfo implements DataWithBackend, RenderRow, RenderList {
  static I18nMap = I18nMap;
  static FieldList = FieldList;
  static ColumnList = ColumnList;

  readonly id: number;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      maxLength: 100,
      disable: Boolean(values?.id),
    }),
    tooltip: i18n.t('meta.Synchronize.GroupIdHelp'),
    rules: [
      { required: true },
      {
        pattern: /^[a-z_0-9]+$/,
        message: i18n.t('meta.Group.InlongGroupIdRules'),
      },
    ],
  })
  @ColumnDecorator()
  @I18n('meta.Synchronize.GroupId')
  inlongGroupId: string;

  @FieldDecorator({
    type: UserSelect,
    extra: i18n.t('meta.Synchronize.InlongGroupOwnersExtra'),
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  })
  @ColumnDecorator()
  @I18n('meta.Synchronize.GroupOwners')
  inCharges: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: false,
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: i18n.t('meta.Consume.Yes'),
          value: true,
        },
        {
          label: i18n.t('meta.Consume.No'),
          value: false,
        },
      ],
    },
  })
  @I18n('meta.Synchronize.SinkMultipleEnable')
  sinkMultipleEnable: boolean;
  @FieldDecorator({
    type: 'radio',
    initialValue: 1,
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: i18n.t('meta.Synchronize.RealTime'),
          value: 1,
        },
        {
          label: i18n.t('meta.Synchronize.Offline'),
          value: 2,
        },
      ],
    },
  })
  @ColumnDecorator({
    width: 200,
    render: text => inlongGroupModeList?.filter(item => item.value === text)?.[0]?.label,
  })
  @I18n('meta.Synchronize.SyncType')
  inlongGroupMode: Number;

  @FieldDecorator({
    type: 'radio',
    initialValue: 0,
    visible: values => values.inlongGroupMode === 2,
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: i18n.t('meta.Synchronize.Conventional'),
          value: 0,
        },
        {
          label: i18n.t('meta.Synchronize.Crontab'),
          value: 1,
        },
      ],
    },
  })
  @I18n('meta.Synchronize.ScheduleType')
  scheduleType: Number;
  @FieldDecorator({
    visible: values => values.inlongGroupMode === 2 && values.scheduleType === 0,
    type: 'select',
    initialValue: 'H',
    name: 'scheduleUnit',
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Year'),
          value: 'Y',
        },
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Month'),
          value: 'M',
        },
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Day'),
          value: 'D',
        },
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Hours'),
          value: 'H',
        },
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Minute'),
          value: 'I',
        },
        {
          label: i18n.t('meta.Synchronize.ScheduleUnit.Single'),
          value: 'O',
        },
      ],
    },
  })
  @I18n('meta.Synchronize.ScheduleUnit')
  scheduleUnit: String;

  @FieldDecorator({
    type: 'input',
    initialValue: 0,
    rules: [{ required: true, pattern: new RegExp(/^[0-9]\d*$/, 'g') }],
    visible: values => values.inlongGroupMode === 2 && values.scheduleType === 0,
    props: values => ({
      suffix: values.scheduleUnit,
    }),
  })
  @I18n('meta.Synchronize.ScheduleInterval')
  scheduleInterval: number;

  @FieldDecorator({
    visible: values => values.inlongGroupMode === 2 && values.scheduleType === 0,
    type: TimePicker,
    initialValue: dayjs('00:00', format),
    name: 'delayTime',
    rules: [{ required: true }],
    props: {
      format: format,
    },
  })
  @I18n('meta.Synchronize.DelayTime')
  delayTime: dayjs.Dayjs;

  @FieldDecorator({
    type: DatePicker.RangePicker,
    props: values => ({
      format: conventionalTimeFormat,
      showTime: true,
      disabledTime: (date: dayjs.Dayjs, type, info: { from?: dayjs.Dayjs }) => {
        return {
          disabledSeconds: () => range(0, 60),
        };
      },
    }),
    visible: values =>
      values.inlongGroupMode === 2 && (values.scheduleType === 0 || values.scheduleType === 1),
    rules: [{ required: true }],
  })
  @I18n('meta.Synchronize.ValidTime')
  time: dayjs.Dayjs[];
  @FieldDecorator({
    visible: values => values.inlongGroupMode === 2 && values.scheduleType === 1,
    type: 'input',
    rules: [{ required: true }],
    props: {},
  })
  @I18n('meta.Synchronize.CronExpression')
  crontabExpression: string;

  @FieldDecorator({
    type: 'radio',
    visible: values => values.inlongGroupMode === 2,
    initialValue: 0,
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: i18n.t('meta.Synchronize.Alone'),
          value: 0,
        },
        {
          label: i18n.t('meta.Synchronize.Self'),
          value: 1,
        },
        {
          label: i18n.t('meta.Synchronize.Parallel'),
          value: 2,
        },
      ],
    },
  })
  @I18n('meta.Synchronize.SelfDependence')
  selfDepend: Number;
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    visible: values => values.selfDepend === 2 && values.inlongGroupMode === 2,
  })
  @I18n('meta.Synchronize.TaskParallelism')
  taskParallelism: string;
  @FieldDecorator({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 100,
    },
  })
  @I18n('meta.Group.InlongGroupIntroduction')
  description: string;

  @FieldDecorator({
    type: 'input',
    initialValue: 'NONE',
    hidden: true,
  })
  @I18n('meta.Group.MQType')
  mqType: string;

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
  @I18n('basic.Status')
  readonly status: string;

  @ColumnDecorator({
    render: text => timestampFormat(text),
  })
  @I18n('basic.CreateTime')
  readonly createTime: string;

  @ColumnDecorator()
  @I18n('basic.Creator')
  readonly creator: string;

  @ColumnDecorator()
  @I18n('basic.Modifier')
  readonly modifier: string;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }

  renderRow() {
    const constructor = this.constructor as typeof SyncDefaultInfo;
    return constructor.FieldList;
  }

  renderList() {
    const constructor = this.constructor as typeof SyncDefaultInfo;
    return constructor.ColumnList;
  }
}
