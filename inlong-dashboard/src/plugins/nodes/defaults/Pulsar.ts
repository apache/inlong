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

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

export default class PulsarNode extends NodeInfo implements DataWithBackend, RenderRow, RenderList {
  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: {
      placeholder: 'pulsar://127.0.0.1:6650,127.0.1.2:6650',
    },
  })
  @I18n('Service URL')
  serviceUrl: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: {
      placeholder: 'http://127.0.0.1:8080,127.0.1.2:8080',
    },
  })
  @I18n('Admin URL')
  adminUrl: string;

  @FieldDecorator({
    type: 'password',
  })
  @I18n('Token')
  token: string;
}
