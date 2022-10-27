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
import { GroupInfo } from '../common/GroupInfo';

const { I18n, FormField } = DataWithBackend;

export default class TubeMqGroup extends GroupInfo implements DataWithBackend {
  @FormField({
    type: 'inputnumber',
    rules: [{ required: true }],
    suffix: i18n.t('meta.Group.TenThousand/Day'),
    props: {
      min: 1,
      precision: 0,
    },
  })
  @I18n('meta.Group.NumberOfAccess')
  dailyRecords: number;

  @FormField({
    type: 'inputnumber',
    rules: [{ required: true }],
    suffix: i18n.t('meta.Group.GB/Day'),
    props: {
      min: 1,
      precision: 0,
    },
  })
  @I18n('meta.Group.AccessSize')
  dailyStorage: number;

  @FormField({
    type: 'inputnumber',
    rules: [{ required: true }],
    suffix: i18n.t('meta.Group.Stripe/Second'),
    props: {
      min: 1,
      precision: 0,
    },
  })
  @I18n('meta.Group.AccessPeakPerSecond')
  peakRecords: number;

  @FormField({
    type: 'inputnumber',
    rules: [{ required: true }],
    suffix: 'Byte',
    props: {
      min: 1,
      precision: 0,
    },
  })
  @I18n('meta.Group.SingleStripMaximumLength')
  maxLength: number;
}
