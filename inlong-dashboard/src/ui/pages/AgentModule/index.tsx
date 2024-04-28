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

import React, { useState } from 'react';
import i18n from '@/i18n';
import AgentModalTag from './AgentModuleTag';
import { Container, PageContainer } from '@/ui/components/PageContainer';
import { Card } from 'antd';
import { useHistory, useParams } from '@/ui/hooks';

const Comp: React.FC = () => {
  const tabList = [
    {
      tab: i18n.t('pages.ModuleAgent.Agent'),
      key: 'AGENT',
      content: <AgentModalTag AgentModalType={'AGENT'} />,
    },
    {
      tab: i18n.t('pages.ModuleAgent.Installer'),
      key: 'INSTALLER',
      content: <AgentModalTag AgentModalType={'INSTALLER'} />,
    },
  ];

  const history = useHistory();

  const { type } = useParams<Record<string, string>>();

  const [module, setModule] = useState(type || tabList[0].key);

  const tabListMap = tabList.reduce(
    (acc, item) => ({
      ...acc,
      [item.key]: item.content,
    }),
    {},
  );

  const onTabsChange = value => {
    setModule(value);
    history.push({
      pathname: `/agentModule`,
      state: { type: value },
    });
  };

  return (
    <PageContainer useDefaultBreadcrumb={false} useDefaultContainer={false}>
      <Container>
        <Card
          tabList={tabList}
          activeTabKey={module}
          onTabChange={key => {
            onTabsChange(key);
          }}
          headStyle={{ border: 'none' }}
          tabProps={{ size: 'middle' }}
        >
          {tabListMap[module]}
        </Card>
      </Container>
    </PageContainer>
  );
};

export default Comp;
