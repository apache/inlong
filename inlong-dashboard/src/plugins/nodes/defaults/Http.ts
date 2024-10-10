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
import i18n from 'i18next';
import { Input } from 'antd';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

export default class HttpNodeInfo
  extends NodeInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
  })
  @I18n('meta.Nodes.Http.BaseUrl')
  baseUrl: string;

  @FieldDecorator({
    type: 'radio',
    rules: [{ required: true }],
    initialValue: false,
    props: {
      options: [
        {
          label: i18n.t('basic.Yes'),
          value: true,
        },
        {
          label: i18n.t('basic.No'),
          value: false,
        },
      ],
    },
  })
  @I18n('meta.Nodes.Http.EnableCredential')
  enableCredential: string;

  @FieldDecorator({
    type: 'input',
  })
  @I18n('meta.Nodes.Http.Username')
  username: string;

  @FieldDecorator({
    type: Input.Password,
  })
  @I18n('meta.Nodes.Http.Password')
  password: string;

  @FieldDecorator({
    type: 'inputnumber',
    initialValue: 1000,
  })
  @I18n('meta.Nodes.Http.MaxConnect')
  maxConnect: number;
}
