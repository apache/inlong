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

import UserSelect from '@/components/UserSelect';
import { DataWithBackend } from '@/metas/DataWithBackend';
import i18n from '@/i18n';
import { statusList, genStatusTag } from './status';
import { groups, defaultValue } from '..';

const { I18n, FormField, TableColumn } = DataWithBackend;

export class GroupDefaultInfo extends DataWithBackend {
  readonly id: number;

  @FormField({
    type: 'input',
    props: {
      maxLength: 32,
    },
    rules: [
      { required: true },
      {
        pattern: /^[a-z_\-\d]+$/,
        message: i18n.t('meta.Group.InlongGroupIdRules'),
      },
    ],
  })
  @TableColumn()
  @I18n('meta.Group.InlongGroupId')
  inlongGroupId: string;

  @FormField({
    type: 'input',
    props: {
      maxLength: 32,
    },
  })
  @I18n('meta.Group.InlongGroupName')
  name: string;

  @FormField({
    type: UserSelect,
    extra: i18n.t('meta.Group.InlongGroupOwnersExtra'),
    rules: [{ required: true }],
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  })
  @TableColumn()
  @I18n('meta.Group.InlongGroupOwners')
  inCharges: string;

  @FormField({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 100,
    },
  })
  @I18n('meta.Group.InlongGroupIntroduction')
  description: string;

  @FormField({
    type: 'radio',
    initialValue: defaultValue,
    rules: [{ required: true }],
    props: {
      options: groups.filter(item => Boolean(item.value)),
    },
  })
  @TableColumn()
  @I18n('meta.Group.MQType')
  mqType: string;

  @FormField({
    type: 'text',
  })
  @I18n('MQ Resource')
  readonly mqResource: string;

  @FormField({
    type: 'select',
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
    visible: false,
  })
  @TableColumn({
    render: text => genStatusTag(text),
  })
  @I18n('basic.Status')
  readonly status: string;

  @TableColumn()
  @I18n('basic.CreateTime')
  readonly createTime: string;

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }
}
