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

import React, { useMemo, useRef } from 'react';
import { Button, Card, Descriptions, Modal, message, Space } from 'antd';
import { parse } from 'qs';
import { PageContainer, Container, FooterToolbar } from '@/components/PageContainer';
import { useParams, useRequest, useLocation, useHistory } from '@/hooks';
import { timestampFormat } from '@/utils';
import request from '@/utils/request';
import Steps from './Steps';
import Access from './Access';
import Consume from './Consume';

const workflowFormat = (applicant, startEvent, taskHistory = []) => {
  const taskHistoryMap = new Map(taskHistory.map(item => [item.name, item]));
  let data = [
    {
      title: '提交申请',
      name: '',
      desc: applicant,
      state: 'COMPLETED',
    },
  ];
  const nextList = [startEvent.next];
  while (nextList.length) {
    const next = nextList.shift();
    data = data.concat(
      next.map(item => {
        if (item.next && item.next.length) {
          nextList.push(item.next);
        }
        return {
          title: nextList.length ? item.displayName : '完成',
          desc: item.approvers?.join(', '),
          name: item.name,
          state: item.state,
          remark: taskHistoryMap.get(item.name)?.remark,
        };
      }),
    );
  }
  return data;
};

const titleNameMap = {
  Applies: '申请单',
  Approvals: '待办审批单',
};

const Comp: React.FC = () => {
  const history = useHistory();
  const location = useLocation();

  const { id } = useParams<{ id: string }>();

  const actived = useMemo<string>(() => parse(location.search.slice(1))?.actived, [
    location.search,
  ]);

  const taskId = useMemo<string>(() => parse(location.search.slice(1))?.taskId, [location.search]);

  const formRef = useRef(null);

  const { data = {} } = useRequest({
    url: `/workflow/detail/${id}`,
    params: {
      taskInstId: taskId,
    },
  });

  const { currentTask, processInfo, workflow, taskHistory } = (data || {}) as any;

  const onApprove = async () => {
    const { remark, form: formData } = await formRef?.current?.onOk();
    const submitData = {
      remark,
    } as any;
    if (formData && Object.keys(formData).length) {
      submitData.form = {
        ...formData,
        formName: currentTask?.formData?.formName,
      };
    }

    await request({
      url: `/workflow/approve/${taskId}`,
      method: 'POST',
      data: submitData,
    });
    history.push('/approvals?actived=Approvals');
    message.success('通过成功');
  };

  const onReject = async () => {
    Modal.confirm({
      title: '确认驳回吗?',
      onOk: async () => {
        const { remark } = await formRef?.current?.onOk(false);
        await request({
          url: `/workflow/reject/${taskId}`,
          method: 'POST',
          data: {
            remark,
          },
        });
        history.push('/approvals?actived=Approvals');
        message.success('驳回成功');
      },
    });
  };

  const onCancel = async () => {
    Modal.confirm({
      title: '确认撤回吗?',
      onOk: async () => {
        // const { remark } = await formRef?.current?.onOk(false);
        await request({
          url: `/workflow/cancel/` + id,
          method: 'POST',
          data: {
            remark: '',
          },
        });
        history.push('/approvals?actived=Applies');
        message.success('撤回成功');
      },
    });
  };

  const stepsData = useMemo(() => {
    if (workflow?.startEvent) {
      return workflowFormat(processInfo?.applicant, workflow.startEvent, taskHistory);
    }
    return [];
  }, [workflow, processInfo, taskHistory]);

  const Footer = () => (
    <>
      {actived === 'Approvals' && currentTask?.state === 'PENDING' && (
        <Space style={{ display: 'flex', justifyContent: 'center' }}>
          <Button type="primary" onClick={onApprove}>
            通过
          </Button>
          <Button onClick={onReject}>驳回</Button>
          <Button onClick={() => history.push('/approvals?actived=Approvals')}>返回</Button>
        </Space>
      )}
      {actived === 'Applies' && processInfo?.state === 'PROCESSING' && (
        <Space style={{ display: 'flex', justifyContent: 'center' }}>
          <Button onClick={onCancel}>撤回</Button>
          <Button onClick={() => history.push('/approvals?actived=Applies')}>返回</Button>
        </Space>
      )}
    </>
  );

  const Form = useMemo(() => {
    return {
      NEW_BUSINESS_WORKFLOW: Access,
      NEW_CONSUMPTION_WORKFLOW: Consume,
    }[processInfo?.name];
  }, [processInfo]);

  // Approval completed
  const isFinished = currentTask?.state === 'APPROVED';
  // Do not display redundant approval information, such as approval cancellation/rejection
  const noExtraForm = currentTask?.state === 'REJECTED' || currentTask?.state === 'CANCELED';

  const suffixContent = [
    {
      type: 'textarea',
      label: '审批意见',
      name: 'remark',
      initialValue: currentTask?.remark,
      props: {
        showCount: true,
        maxLength: 100,
        disabled: isFinished || noExtraForm,
      },
    },
  ];

  const formProps = {
    defaultData: data,
    isViwer: actived !== 'Approvals',
    isAdminStep: currentTask?.name === 'ut_admin',
    isFinished,
    noExtraForm,
    suffixContent,
  };

  return (
    <PageContainer
      breadcrumb={[{ name: `${titleNameMap[actived] || '流程单'}${id}` }]}
      useDefaultContainer={false}
    >
      <div style={{ display: 'flex' }}>
        <Container style={{ flex: 1, marginRight: 20 }}>
          <Card title={workflow?.displayName}>
            <Descriptions>
              <Descriptions.Item label="申请人">{processInfo?.applicant}</Descriptions.Item>
              <Descriptions.Item label="申请时间">
                {processInfo?.startTime ? timestampFormat(processInfo?.startTime) : ''}
              </Descriptions.Item>
            </Descriptions>

            {Form && <Form ref={formRef} {...formProps} />}
          </Card>
        </Container>
        <Container style={{ flex: '0 0 200px' }}>
          <Card title="审批流程" style={{ height: '100%' }}>
            <Steps data={stepsData} />
          </Card>
        </Container>
      </div>
      <FooterToolbar extra={<Footer />} />
    </PageContainer>
  );
};

export default Comp;
