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
import i18n from '@/i18n';
import rulesPattern from '@/utils/pattern';
import { SourceInfo } from '../common/SourceInfo';

const { I18n, FormField, TableColumn } = DataWithBackend;

export default class PulsarSource extends SourceInfo implements DataWithBackend {
  @FormField({
    type: 'input',
    rules: [
      {
        pattern: rulesPattern.ip,
        message: i18n.t('meta.Sources.File.IpRule'),
        required: true,
      },
    ],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @TableColumn()
  @I18n('meta.Sources.File.DataSourceIP')
  agentIp: string;

  @FormField({
    type: 'input',
    tooltip: i18n.t('meta.Sources.File.FilePathHelp'),
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @TableColumn()
  @I18n('meta.Sources.File.FilePath')
  pattern: string;

  @FormField({
    type: 'input',
    tooltip: i18n.t('meta.Sources.File.TimeOffsetHelp'),
    props: values => ({
      disabled: values?.status === 101,
    }),
  })
  @I18n('meta.Sources.File.TimeOffset')
  timeOffset: string;
}
