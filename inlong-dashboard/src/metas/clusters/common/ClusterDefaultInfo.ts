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
import UserSelect from '@/components/UserSelect';
import { clusters, defaultValue } from '..';

const { I18n, FormField, TableColumn } = DataWithBackend;

export class ClusterDefaultInfo extends DataWithBackend {
  id: number;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: {
      maxLength: 128,
    },
  })
  @TableColumn()
  @I18n('pages.Clusters.Name')
  name: string;

  @FormField({
    type: clusters.length > 3 ? 'select' : 'radio',
    initialValue: defaultValue,
    rules: [{ required: true }],
    props: {
      options: clusters
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
        })),
    },
  })
  @TableColumn()
  @I18n('pages.Clusters.Type')
  type: string;

  @FormField({
    type: 'select',
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      filterOption: false,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/tag/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.clusterTag,
              value: item.clusterTag,
            })),
        },
      },
    },
  })
  @TableColumn()
  @I18n('pages.Clusters.Tag')
  clusterTags: string;

  @FormField({
    type: UserSelect,
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  })
  @TableColumn()
  @I18n('pages.Clusters.InCharges')
  inCharges: string;

  @FormField({
    type: 'textarea',
    props: {
      maxLength: 256,
    },
  })
  @I18n('pages.Clusters.Description')
  description: string;

  version?: number;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }
}
