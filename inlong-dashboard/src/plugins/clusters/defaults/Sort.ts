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
import { ClusterInfo } from '../common/ClusterInfo';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

const EngineVersions = [
  { label: '1.13', value: '1.13', type: 'Flink' },
  { label: '1.15', value: '1.15', type: 'Flink' },
];

export default class SortCluster
  extends ClusterInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'select',
    initialValue: '',
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: 'Flink',
          value: 'Flink',
        },
        {
          label: 'Spark',
          value: 'Spark',
        },
      ],
    },
  })
  @I18n('pages.Sort.EngineType')
  engineType: string;

  @FieldDecorator({
    type: 'select',
    initialValue: '',
    rules: [{ required: true }],
    props: values => ({
      options: EngineVersions.filter(e => values!.engineType === e.type).map(item => ({
        label: item.label,
        value: item.value,
      })),
    }),
  })
  @I18n('pages.Sort.EngineVersion')
  engineVersion: string;

  @FieldDecorator({
    type: 'select',
    initialValue: '',
    rules: [{ required: true }],
    props: {
      options: [
        {
          label: 'Apache Yarn',
          value: 'ApacheYarn',
        },
        {
          label: 'Kubernetes',
          value: 'Kubernetes',
        },
      ],
    },
  })
  @I18n('pages.Sort.ResourceType')
  resourceType: string;

  @FieldDecorator({
    type: 'input',
    initialValue: '127.0.0.1',
    rules: [{ required: false }],
    props: {
      placeholder: '127.0.0.1',
    },
    visible: values => values!.resourceType === 'ApacheYarn',
  })
  @I18n('pages.Sort.yarnRestHost')
  yarnRestHost: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: '8081',
    rules: [{ required: false }],
    props: {
      placeholder: '8081',
    },
    visible: values => values!.resourceType === 'ApacheYarn',
  })
  @I18n('pages.Sort.yarnRestPort')
  yarnRestPort: number;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: '1',
    rules: [{ required: true }],
    props: {
      placeholder: '1',
    },
  })
  @I18n('pages.Sort.Parallelism')
  parallelism: number;

  @FieldDecorator({
    type: 'input',
    initialValue: '127.0.0.1:10081',
    rules: [{ required: true }],
    props: {
      placeholder: '127.0.0.1:10081',
    },
  })
  @I18n('pages.Sort.AuditProxy')
  auditProxy: string;
}
