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
import type { FieldItemType } from '@/metas/common';
import { genFields, genForm, genTable } from '@/metas/common';
import { statusList, genStatusTag } from './common/status';
import { autoPush } from './autoPush';
import { binLog } from './binLog';
import { file } from './file';

const allSources = [
  {
    label: 'MySQL BinLog',
    value: 'MYSQL_BINLOG',
    fields: binLog,
  },
  {
    label: 'File',
    value: 'FILE',
    fields: file,
  },
  {
    label: 'Auto Push',
    value: 'AUTO_PUSH',
    fields: autoPush,
  },
];

const defaultCommonFields: FieldItemType[] = [
  {
    name: 'sourceName',
    type: 'input',
    label: i18n.t('meta.Sources.Name'),
    rules: [{ required: true }],
    props: values => ({
      disabled: !!values.id,
      maxLength: 128,
    }),
    _renderTable: true,
  },
  {
    name: 'sourceType',
    type: 'radio',
    label: i18n.t('meta.Sources.Type'),
    rules: [{ required: true }],
    initialValue: allSources[0].value,
    props: values => ({
      disabled: !!values.id,
      options: allSources.map(item => ({
        label: item.label,
        value: item.value,
      })),
    }),
  },
  {
    name: 'status',
    type: 'select',
    label: i18n.t('basic.Status'),
    props: {
      allowClear: true,
      options: statusList,
      dropdownMatchSelectWidth: false,
    },
    visible: false,
    _renderTable: {
      render: text => genStatusTag(text),
    },
  },
];

export const sources = allSources.map(item => {
  const itemFields = defaultCommonFields.concat(item.fields);
  const fields = genFields(itemFields);

  return {
    ...item,
    fields,
    form: genForm(fields),
    table: genTable(fields),
    toFormValues: null,
    toSubmitValues: null,
  };
});
