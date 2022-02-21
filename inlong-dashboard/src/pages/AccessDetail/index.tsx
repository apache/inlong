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

import React, { useMemo, useState, useRef } from 'react';
import { Tabs } from 'antd';
import { PageContainer } from '@/components/PageContainer';
import { useParams, useRequest } from '@/hooks';
import { useTranslation } from 'react-i18next';
import Info from './Info';
import DataSources from './DataSources';
import DataStream from './DataStream';
import DataStorage from './DataStorage';
import Audit from './Audit';

const Comp: React.FC = () => {
  const { t } = useTranslation();
  const { id } = useParams<{ id: string }>();

  const { data } = useRequest(`/group/get/${id}`, {
    refreshDeps: [id],
  });

  const extraRef = useRef<HTMLDivElement>();

  const isReadonly = useMemo(() => [0, 101, 102].includes(data?.status), [data]);
  const middlewareType = data?.middlewareType;

  const list = useMemo(
    () =>
      [
        {
          label: t('pages.AccessDetail.Business'),
          value: 'groupInfo',
          content: Info,
        },
        {
          label: middlewareType === 'PULSAR' ? 'TOPIC' : t('pages.AccessDetail.DataStreams'),
          value: 'dataStream',
          content: DataStream,
        },
        {
          label: t('pages.AccessDetail.DataSources'),
          value: 'dataSources',
          content: DataSources,
          hidden: isReadonly,
        },
        {
          label: t('pages.AccessDetail.DataStorages'),
          value: 'streamSink',
          content: DataStorage,
          hidden: isReadonly,
        },
        {
          label: t('pages.AccessDetail.Audit'),
          value: 'audit',
          content: Audit,
          hidden: isReadonly,
        },
      ].filter(item => !item.hidden),
    [isReadonly, middlewareType, t],
  );

  const [actived, setActived] = useState(list[0].value);

  return (
    <PageContainer breadcrumb={[{ name: `${t('pages.AccessDetail.BusinessDetail')}${id}` }]}>
      <Tabs
        activeKey={actived}
        onChange={val => setActived(val)}
        tabBarExtraContent={<div ref={extraRef} />}
      >
        {list.map(({ content: Content, ...item }) => (
          <Tabs.TabPane tab={item.label} key={item.value}>
            <Content
              inlongGroupId={id}
              isActive={actived === item.value}
              readonly={isReadonly}
              extraRef={extraRef}
              middlewareType={middlewareType}
            />
          </Tabs.TabPane>
        ))}
      </Tabs>
    </PageContainer>
  );
};

export default Comp;
