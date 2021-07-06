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
import Info from './Info';
import DataSources from './DataSources';
import DataStream from './DataStream';
import DataStorage from './DataStorage';

const Comp: React.FC = () => {
  const { id } = useParams<{ id: string }>();

  const { data } = useRequest(`/business/get/${id}`, {
    refreshDeps: [id],
  });

  const extraRef = useRef<HTMLDivElement>();

  const isReadonly = useMemo(() => [0, 101, 102].includes(data?.status), [data]);

  const list = useMemo(
    () =>
      [
        {
          label: '业务信息',
          value: 'bussinessInfo',
          content: Info,
        },
        {
          label: '数据流',
          value: 'dataStream',
          content: DataStream,
        },
        {
          label: '数据源',
          value: 'dataSources',
          content: DataSources,
          hidden: isReadonly,
        },
        {
          label: '流向',
          value: 'dataStorage',
          content: DataStorage,
          hidden: isReadonly,
        },
      ].filter(item => !item.hidden),
    [isReadonly],
  );

  const [actived, setActived] = useState(list[0].value);

  return (
    <PageContainer breadcrumb={[{ name: `业务详情${id}` }]}>
      <Tabs
        activeKey={actived}
        onChange={val => setActived(val)}
        tabBarExtraContent={<div ref={extraRef} />}
      >
        {list.map(({ content: Content, ...item }) => (
          <Tabs.TabPane tab={item.label} key={item.value}>
            <Content
              bid={id}
              isActive={actived === item.value}
              readonly={isReadonly}
              extraRef={extraRef}
            />
          </Tabs.TabPane>
        ))}
      </Tabs>
    </PageContainer>
  );
};

export default Comp;
