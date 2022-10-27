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
import { statusList, genStatusTag } from './status';
import { sinks, defaultValue } from '..';

const { I18n, FormField, TableColumn } = DataWithBackend;

export class SinkDefaultInfo extends DataWithBackend {
  readonly id: number;

  @FormField({
    // This field is not visible or editable, but form value should exists.
    type: 'text',
    hidden: true,
  })
  @I18n('inlongGroupId')
  readonly inlongGroupId: string;

  @FormField({
    type: 'select',
    props: values => ({
      disabled: Boolean(values.id),
      options: {
        requestService: {
          url: '/stream/list',
          method: 'POST',
          data: {
            pageNum: 1,
            pageSize: 1000,
            inlongGroupId: values.inlongGroupId,
          },
        },
        requestParams: {
          formatResult: result =>
            result?.list.map(item => ({
              label: item.inlongStreamId,
              value: item.inlongStreamId,
            })) || [],
        },
      },
    }),
    rules: [{ required: true }],
  })
  @TableColumn()
  @I18n('pages.GroupDetail.Sink.DataStreams')
  inlongStreamId: string;

  @FormField({
    type: 'input',
    rules: [
      { required: true },
      {
        pattern: /^[a-zA-Z][a-zA-Z0-9_-]*$/,
        message: i18n.t('meta.Sinks.SinkNameRule'),
      },
    ],
    props: values => ({
      disabled: !!values.id,
      maxLength: 128,
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.SinkName')
  sinkName: string;

  @FormField({
    type: sinks.length > 3 ? 'select' : 'radio',
    label: i18n.t('meta.Sinks.SinkType'),
    rules: [{ required: true }],
    initialValue: defaultValue,
    props: values => ({
      dropdownMatchSelectWidth: false,
      disabled: !!values.id,
      options: sinks
        .filter(item => item.value)
        .map(item => ({
          label: item.label,
          value: item.value,
        })),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.SinkType')
  sinkType: string;

  @FormField({
    type: 'textarea',
    props: {
      showCount: true,
      maxLength: 300,
    },
  })
  @I18n('meta.Sinks.Description')
  description: string;

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

  parse(data) {
    return data;
  }

  stringify(data) {
    return data;
  }
}
