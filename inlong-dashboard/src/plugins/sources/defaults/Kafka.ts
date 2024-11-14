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
import { SourceInfo } from '../common/SourceInfo';
import i18n from '@/i18n';
import rulesPattern from '@/core/utils/pattern';

const { I18n } = DataWithBackend;
const { FieldDecorator, SyncField, IngestionField } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class KafkaSource
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
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
      placeholder: 'localhost:9092',
    }),
  })
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  @I18n('meta.Sources.Kafka.BootstrapServers')
  bootstrapServers: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  @I18n('Topic')
  topic: string;

  @FieldDecorator({
    type: 'select',
    initialValue: 'latest',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
      options: [
        {
          label: 'Earliest',
          value: 'earliest',
        },
        {
          label: 'Latest',
          value: 'latest',
        },
        {
          label: 'None',
          value: 'none',
        },
      ],
    }),
  })
  @IngestionField()
  @I18n('meta.Sources.Kafka.autoOffsetReset')
  autoOffsetReset: string;

  @FieldDecorator({
    type: 'input',
    tooltip: i18n.t('meta.Sources.Kafka.partitionOffsetsHelp'),
    props: values => ({
      disabled: values?.status === 101,
      placeholder: '0#0_1#0_2#0',
    }),
  })
  @SyncField()
  @IngestionField()
  @ColumnDecorator()
  @I18n('meta.Sources.Kafka.partitionOffsets')
  partitionOffsets: string;

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
