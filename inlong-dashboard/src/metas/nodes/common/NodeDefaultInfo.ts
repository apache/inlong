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
import { nodes, defaultValue } from '..';

const { I18n, FormField, TableColumn } = DataWithBackend;

export class NodeDefaultInfo extends DataWithBackend {
  readonly id: number;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: {
      maxLength: 128,
    },
  })
  @TableColumn()
  @I18n('meta.Nodes.Name')
  name: string;

  @FormField({
    type: nodes.length > 3 ? 'select' : 'radio',
    initialValue: defaultValue,
    rules: [{ required: true }],
    props: {
      options: nodes
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
        })),
    },
  })
  @TableColumn()
  @I18n('meta.Nodes.Type')
  type: string;

  @FormField({
    type: UserSelect,
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  })
  @TableColumn()
  @I18n('meta.Nodes.Owners')
  inCharges: string;

  clusterTags: string;

  @FormField({
    type: 'textarea',
    props: {
      maxLength: 256,
    },
  })
  @I18n('meta.Nodes.Description')
  description?: string;

  readonly version?: number;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }
}
