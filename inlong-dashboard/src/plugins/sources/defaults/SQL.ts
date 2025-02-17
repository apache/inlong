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

export default class SQLSource
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
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @I18n('clusterId')
  @IngestionField()
  clusterId: number;

  @FieldDecorator({
    type: 'select',
    rules: [
      {
        pattern: rulesPattern.ip,
        message: i18n.t('meta.Sources.File.IpRule'),
        required: true,
      },
    ],
    props: values => ({
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
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.ip,
              value: item.ip,
            })),
        },
      },
    }),
  })
  @ColumnDecorator()
  @IngestionField()
  @I18n('meta.Sources.File.DataSourceIP')
  agentIp: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      allowClear: true,
      filterOption: false,
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/node/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
            type: 'SQL',
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
    }),
  })
  @IngestionField()
  @ColumnDecorator()
  @I18n('meta.Sources.COS.DataNode')
  dataNodeName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @IngestionField()
  @ColumnDecorator()
  @I18n('meta.Sinks.SQL.Sql')
  sql: string;

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
      ],
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.File.Cycle')
  cycleUnit: string;

  @FieldDecorator({
    type: 'input',
    tooltip: i18n.t('meta.Sources.File.TimeOffsetHelp'),
    initialValue: '-1h',
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
  maxInstanceCount: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 1000,
    rules: [{ required: true }],
    props: values => ({
      disabled: Boolean(values.id),
    }),
  })
  @IngestionField()
  @I18n('meta.Sinks.SQL.FetchSize')
  fetchSize: string;

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
