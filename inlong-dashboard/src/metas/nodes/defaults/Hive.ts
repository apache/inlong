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

import i18n from '@/i18n';
import { DataWithBackend } from '@/metas/DataWithBackend';
import { NodeInfo } from '../common/NodeInfo';

const { I18n, FormField } = DataWithBackend;

export default class HiveNode extends NodeInfo implements DataWithBackend {
  @FormField({
    type: 'input',
    rules: [{ required: true }],
    initialValue: 'jdbc:hive2://127.0.0.1:10000',
  })
  @I18n('JDBC URL')
  jdbcUrl: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    tooltip: i18n.t('meta.Sinks.DataPathHelp'),
    initialValue: 'hdfs://127.0.0.1:9000/user/hive/warehouse/default',
  })
  @I18n('meta.Sinks.Hive.DataPath')
  dataPath: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    tooltip: i18n.t('meta.Sinks.Hive.ConfDirHelp'),
    initialValue: '/usr/hive/conf',
  })
  @I18n('meta.Sinks.Hive.ConfDir')
  hiveConfDir: string;
}
