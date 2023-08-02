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

import React, { useMemo } from 'react';
import { Divider } from 'antd';
import i18n from '@/i18n';
import { useLoadMeta, SyncMetaType } from '@/plugins';

export const useSyncFormContent = ({ mqType = '' }) => {
  const { Entity } = useLoadMeta<SyncMetaType>('sync', mqType);

  const entityFields = useMemo(() => {
    return Entity ? new Entity().renderRow() : [];
  }, [Entity]);

  return entityFields?.map(item => {
    const obj = { ...item, col: 12 };

    obj.type = 'text';
    delete obj.rules;
    delete obj.extra;
    if ((obj.suffix as any)?.type) {
      (obj.suffix as any).type = 'text';
      delete (obj.suffix as any).rules;
    }

    return obj;
  });
};

export const getSyncFormContent = ({ isViwer, suffixContent, syncFormContent = [] }) => {
  const array = [
    {
      type: <Divider orientation="left">{i18n.t('pages.Approvals.Type.Group')}</Divider>,
    },
    ...syncFormContent,
  ];

  return isViwer
    ? array
    : array.concat([
        {
          type: (
            <Divider orientation="left">
              {i18n.t('pages.ApprovalDetail.GroupConfig.ApprovalInformation')}
            </Divider>
          ),
        },
        ...suffixContent.map(item => ({ ...item, col: 12 })),
      ]);
};
