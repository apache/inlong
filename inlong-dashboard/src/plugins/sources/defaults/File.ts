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
import rulesPattern from '@/core/utils/pattern';
import { SourceInfo } from '../common/SourceInfo';
import MultiSelectWithALL, { ALL_OPTION_VALUE } from '@/ui/components/MultiSelectWithAll';

const { I18n } = DataWithBackend;
const { FieldDecorator, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class PulsarSource
  extends SourceInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
      showSearch: true,
      allowClear: true,
      filterOption: false,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/list',
          method: 'POST',
          data: {
            keyword,
            type: 'AGENT',
            clusterTag: values.clusterTag,
            pageNum: 1,
            pageSize: 10,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.displayName,
              value: item.name,
            })),
        },
      },
      onChange: (value, option) => {
        return {
          clusterId: option.id,
        };
      },
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.ClusterName')
  inlongClusterName: string;

  @FieldDecorator({
    type: 'text',
    hidden: true,
  })
  @I18n('clusterId')
  @IngestionField()
  clusterId: number;

  @FieldDecorator({
    type: MultiSelectWithALL,
    tooltip: i18n.t('meta.Sources.File.FileIpHelp'),
    rules: [
      {
        pattern: rulesPattern.fileIp,
        message: i18n.t('meta.Sources.File.IpRule'),
        required: true,
      },
    ],
    props: values => ({
      mode: 'multiple',
      disabled: Boolean(values.id),
      showSearch: true,
      allowClear: true,
      filterOption: false,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/node/list',
          method: 'POST',
          data: {
            keyword,
            parentId: values.clusterId,
            pageNum: 1,
            pageSize: 10,
          },
        }),
        requestParams: {
          formatResult: result => {
            const allOption = {
              label: ALL_OPTION_VALUE,
              value: ALL_OPTION_VALUE,
            };
            return result?.list
              ? [
                  allOption,
                  ...result.list.map(item => ({
                    label: item.ip,
                    value: item.ip,
                  })),
                ]
              : [allOption];
          },
        },
      },
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.DataSourceIP')
  agentIp: string[] | string;

  @FieldDecorator({
    type: 'input',
    tooltip: i18n.t('meta.Sources.File.FilePathHelp'),
    rules: [
      { required: true },
      {
        pattern: /^\S*$/,
        message: i18n.t('meta.Sources.File.FilePathPatternHelp'),
      },
    ],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.FilePath')
  pattern: string;

  @FieldDecorator({
    type: 'inputnumber',
    rules: [{ required: true }],
    initialValue: 20,
    props: values => ({
      min: 1,
      max: 100,
      precision: 0,
      disabled: Boolean(values.id),
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.MaxFileCount')
  maxFileCount: number;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'H',
    props: values => ({
      disabled: Boolean(values.id),
      options: [
        {
          label: i18n.t('meta.Sources.File.Cycle.Day'),
          value: 'D',
        },
        {
          label: i18n.t('meta.Sources.File.Cycle.Hour'),
          value: 'H',
        },
        {
          label: i18n.t('meta.Sources.File.Cycle.Minute'),
          value: 'm',
        },
        {
          label: i18n.t('meta.Sources.File.Cycle.RealTime'),
          value: 'R',
        },
      ],
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.Cycle')
  cycleUnit: string;

  @FieldDecorator({
    type: 'input',
    tooltip: i18n.t('meta.Sources.File.TimeOffsetHelp'),
    initialValue: '0h',
    rules: [
      {
        pattern: /-?\d+[mhd]$/,
        required: true,
        message: i18n.t('meta.Sources.File.TimeOffsetRules'),
      },
    ],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.TimeOffset')
  timeOffset: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'GMT+8:00',
    props: values => ({
      disabled: Boolean(values.id),
      options: [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11,
        -12,
      ].map(item => ({
        label: Math.sign(item) === 1 || Math.sign(item) === 0 ? `GMT+${item}:00` : `GMT${item}:00`,
        value: Math.sign(item) === 1 || Math.sign(item) === 0 ? `GMT+${item}:00` : `GMT${item}:00`,
      })),
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.TimeZone')
  dataTimeZone: string;
}
