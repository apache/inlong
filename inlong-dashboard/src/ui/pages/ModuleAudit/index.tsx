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
import { Card } from 'antd';
import { PageContainer, Container } from '@/ui/components/PageContainer';
import { useHistory, useParams } from '@/ui/hooks';
import i18n from '@/i18n';
import IpModule, { ipModule as ipModuleName } from './IpModule';
import IdModule, { idModule as idModuleName } from './IdModule';
import AuditModule, { auditModule as auditModuleName } from '@/ui/pages/ModuleAudit/AuditModule';
const tabList = [
  {
    tab: i18n.t('pages.ModuleAudit.Id'),
    key: idModuleName,
    content: <IdModule />,
  },
  {
    tab: i18n.t('pages.ModuleAudit.Ip'),
    key: ipModuleName,
    content: <IpModule />,
  },
  {
    tab: i18n.t('pages.ModuleAudit.Metric'),
    key: auditModuleName,
    content: <AuditModule />,
  },
];

const tabListMap = tabList.reduce(
  (acc, item) => ({
    ...acc,
    [item.key]: item.content,
  }),
  {},
);

const Comp: React.FC = () => {
  const history = useHistory();
  const { type } = useParams<Record<string, string>>();

  const [module, setModule] = useState(type || tabList[0].key);

  const onTabsChange = value => {
    setModule(value);
    history.push({
      pathname: `/system/${value}`,
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
