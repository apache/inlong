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

import { useMemo } from 'react';
import { useLoadMeta, ConsumeMetaType } from '@/metas';
import { excludeObjectArray } from '@/utils';
import React from 'react';
import { Table } from 'antd';
import i18n from '@/i18n';

export const useFormContent = ({ mqType, editing, isCreate }) => {
  const { Entity } = useLoadMeta<ConsumeMetaType>('consume', mqType);

  const entityFields = useMemo(() => {
    return Entity ? new Entity().renderRow() : [];
  }, [Entity]);

  const excludeKeys = isCreate ? ['masterUrl'] : [];
  const fields = excludeObjectArray(excludeKeys, entityFields);

  return isCreate
    ? fields
    : fields.map(item => ({
        ...item,
        type: transType(editing, item),
        suffix:
          typeof item.suffix === 'object' && !editing
            ? {
                ...item.suffix,
                type: 'text',
              }
            : item.suffix,
        extra: null,
      }));
};

export const getFormContent = ({ clusterInfos, isCreate, formContent }) => {
  const array = [
    ...formContent,
    {
      type: <label>{i18n.t('pages.ConsumeDetail.ClusterInfo')}</label>,
    },
    {
      type: (
        <Table
          size="small"
          columns={[
            { title: 'name', dataIndex: 'name' },
            { title: 'type', dataIndex: 'type' },
            { title: 'serviceUrl', dataIndex: 'url' },
            { title: 'adminUrl', dataIndex: 'adminUrl' },
          ]}
          dataSource={clusterInfos}
          rowKey="name"
        />
      ),
    },
  ];
  return isCreate ? formContent : array;
};

function transType(editing: boolean, conf) {
  const arr = [
    {
      name: ['isDlq', 'deadLetterTopic', 'isRlq', 'retryLetterTopic'],
      as: 'text',
      active: !editing,
    },
  ].reduce((acc, cur) => {
    return acc.concat(Array.isArray(cur.name) ? cur.name.map(name => ({ ...cur, name })) : cur);
  }, []);

  const map = new Map(arr.map(item => [item.name, item]));
  if (map.has(conf.name)) {
    const item = map.get(conf.name);
    return item.active ? item.as : conf.type;
  }

  return 'text';
}
