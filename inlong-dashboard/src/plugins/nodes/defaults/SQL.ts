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
import { NodeInfo } from '../common/NodeInfo';
import rulesPattern from '@/core/utils/pattern';
import i18n from '@/i18n';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

export default class SQLNode extends NodeInfo implements DataWithBackend, RenderRow, RenderList {
  @FieldDecorator({
    type: 'input',
    rules: [
      {
        required: true,
        pattern: rulesPattern.sql,
        message: i18n.t('meta.SQL.Check'),
      },
    ],
    props: {
      placeholder:
        'jdbc:mysql://xxx.xxx.xxx.xxx:3306/data_base_name?useSSL=false&serverTimezone=UTC',
    },
  })
  @I18n('meta.Nodes.MySQL.Url')
  url: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
  })
  @I18n('meta.Nodes.MySQL.Username')
  username: string;

  @FieldDecorator({
    type: 'password',
    rules: [{ required: true }],
  })
  @I18n('meta.Nodes.MySQL.Password')
  token: string;
}
