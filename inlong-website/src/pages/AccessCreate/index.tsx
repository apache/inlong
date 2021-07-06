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

import React, { useState, useRef } from 'react';
import { Button, Card, message, Steps, Space } from 'antd';
import { parse } from 'qs';
import { PageContainer, Container, FooterToolbar } from '@/components/PageContainer';
import { useHistory, useLocation, useSet } from '@/hooks';
import Bussiness from './Bussiness';
import DataStream from './DataStream';

const { Step } = Steps;

const Create: React.FC = () => {
  const history = useHistory();

  const location = useLocation();

  const qs = parse(location.search.slice(1));

  const [current, setCurrent] = useState(+qs.step || 0);
  const [, { add: addOpened, has: hasOpened }] = useSet([current]);
  const [confirmLoading, setConfirmLoading] = useState(false);

  const [bid, setBid] = useState(qs.bid);

  const bussinessRef = useRef(null);
  const dataStreamRef = useRef(null);

  const steps = [
    {
      title: '业务信息',
      content: <Bussiness ref={bussinessRef} bid={bid} />,
      useCache: true,
      ref: bussinessRef,
    },
    {
      title: '数据流',
      content: <DataStream ref={dataStreamRef} bid={bid} />,
      useCache: true,
      ref: dataStreamRef,
    },
  ];

  const onOk = async current => {
    const currentStepObj = steps[current] as any;
    const onOk = currentStepObj.ref?.current?.onOk;
    setConfirmLoading(true);
    try {
      const result = onOk && (await onOk());
      if (current === 0) {
        setBid(result);
        history.push({
          search: `?bid=${result}&step=1`,
        });
      }
    } finally {
      setConfirmLoading(false);
    }
  };

  const onSubmit = async current => {
    await onOk(current).catch(err => {
      if (err?.errorFields?.length) {
        message.error('请检查表单完整性');
      }
      return Promise.reject(err);
    });
    message.success('提交成功');
    history.push('/access');
  };

  const Footer = () => (
    <Space style={{ display: 'flex', justifyContent: 'center' }}>
      {current > 0 && (
        <Button disabled={confirmLoading} onClick={() => setCurrent(current - 1)}>
          上一步
        </Button>
      )}
      {current !== steps.length - 1 && (
        <Button
          type="primary"
          loading={confirmLoading}
          onClick={async () => {
            await onOk(current).catch(err => {
              if (err?.errorFields?.length) {
                message.error('请检查表单完整性');
              }
              return Promise.reject(err);
            });

            const newCurrent = current + 1;
            setCurrent(newCurrent);
            if (!hasOpened(newCurrent)) addOpened(newCurrent);
          }}
        >
          下一步
        </Button>
      )}
      {current === steps.length - 1 && (
        <Button type="primary" onClick={() => onSubmit(current)}>
          提交审批
        </Button>
      )}
      <Button onClick={() => history.push('/access')}>返回</Button>
    </Space>
  );

  return (
    <PageContainer breadcrumb={[{ name: '新建接入' }]} useDefaultContainer={false}>
      <Steps current={current} size="small" style={{ marginBottom: 20, width: 400 }}>
        {steps.map(item => (
          <Step key={item.title} title={item.title} />
        ))}
      </Steps>

      <Container>
        <Card>
          {steps.map((item, index) => (
            // Lazy load the content of the step, and at the same time make the loaded useCache content not destroy
            <div key={item.title} style={{ display: `${index === current ? 'block' : 'none'}` }}>
              {index === current || (item.useCache && hasOpened(index)) ? item.content : null}
            </div>
          ))}
        </Card>
      </Container>

      <FooterToolbar extra={<Footer />} />
    </PageContainer>
  );
};

export default Create;
