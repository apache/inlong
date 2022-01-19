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

import React, { useCallback, useState } from 'react';
import { Modal, message, Button, Collapse, Popover, Timeline, Pagination, Empty } from 'antd';
import { ModalProps } from 'antd/es/modal';
import HighTable from '@/components/HighTable';
import request from '@/utils/request';
import { useTranslation } from 'react-i18next';
import { useRequest, useUpdateEffect } from '@/hooks';
import { timestampFormat } from '@/utils';
import StatusTag from '@/components/StatusTag';

const { Panel } = Collapse;

export interface Props extends ModalProps {
  inlongGroupId?: string;
}

const Comp: React.FC<Props> = ({ inlongGroupId, ...modalProps }) => {
  const { t } = useTranslation();

  const [options, setOptions] = useState({
    pageNum: 1,
    pageSize: 5,
  });

  const { run: getData, data } = useRequest(
    {
      url: '/workflow/listTaskExecuteLogs',
      params: {
        ...options,
        inlongGroupId: inlongGroupId,
        processNames: 'CREATE_BUSINESS_RESOURCE,CREATE_DATASTREAM_RESOURCE',
        taskType: 'ServiceTask',
      },
    },
    {
      manual: true,
    },
  );

  const onChange = useCallback((pageNum, pageSize) => {
    setOptions(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  }, []);

  const reRun = useCallback(
    ({ taskInstId }) => {
      Modal.confirm({
        title: t('pages.AccessDashboard.ExecutionLogModal.ConfirmThatItIsRe-executed'),
        onOk: async () => {
          await request({
            url: `/workflow/complete/` + taskInstId,
            method: 'POST',
            data: {
              remark: '',
            },
          });
          await getData(inlongGroupId);
          message.success(t('pages.AccessDashboard.ExecutionLogModal.Re-executingSuccess'));
        },
      });
    },
    [getData, inlongGroupId, t],
  );

  useUpdateEffect(() => {
    if (modalProps.visible) {
      getData();
    } else {
      setOptions(prev => ({
        ...prev,
        pageNum: 1,
      }));
    }
  }, [modalProps.visible, options]);

  const columns = [
    {
      title: t('pages.AccessDashboard.ExecutionLogModal.TaskType'),
      dataIndex: 'taskDisplayName',
    },
    {
      title: t('pages.AccessDashboard.ExecutionLogModal.RunResults'),
      dataIndex: 'state',
      render: (text, record) => (
        <>
          <div>
            {record.state === 'COMPLETED' ? (
              <StatusTag
                type={'success'}
                title={t('pages.AccessDashboard.ExecutionLogModal.Success')}
              />
            ) : record.state === 'FAILED' ? (
              <StatusTag type={'error'} title={t('pages.AccessDashboard.ExecutionLogModal.Fail')} />
            ) : record.state === 'SKIPPED' ? (
              <StatusTag
                type={'primary'}
                title={t('pages.AccessDashboard.ExecutionLogModal.Skip')}
              />
            ) : (
              <StatusTag
                type={'warning'}
                title={t('pages.AccessDashboard.ExecutionLogModal.Processing')}
              />
            )}
          </div>
        </>
      ),
    },
    {
      title: t('pages.AccessDashboard.ExecutionLogModal.ExecuteLog'),
      dataIndex: 'listenerExecutorLogs',
      width: 400,
      render: text =>
        text?.length ? (
          <Popover
            content={
              <Timeline mode={'left'} style={{ margin: 20 }}>
                {text.map(item => (
                  <Timeline.Item key={item.id}>{item.description}</Timeline.Item>
                ))}
              </Timeline>
            }
            overlayStyle={{ maxWidth: 750 }}
          >
            <div style={{ height: 45, overflow: 'hidden' }}>{text[0]?.description}</div>
          </Popover>
        ) : null,
    },
    {
      title: t('pages.AccessDashboard.ExecutionLogModal.EndTime'),
      dataIndex: 'endTime',
      render: (text, record) => record.endTime && timestampFormat(record.endTime),
    },
    {
      title: t('basic.Operating'),
      dataIndex: 'actions',
      render: (text, record) => (
        <>
          {record?.state && record.state === 'FAILED' && (
            <Button type="link" onClick={() => reRun(record)}>
              {t('pages.AccessDashboard.ExecutionLogModal.CarriedOut')}
            </Button>
          )}
        </>
      ),
    },
  ];
  return (
    <Modal
      {...modalProps}
      title={t('pages.AccessDashboard.ExecutionLogModal.ExecuteLog')}
      width={1024}
      footer={null}
    >
      {data?.list?.length ? (
        <>
          <Collapse accordion defaultActiveKey={[data.list[0]?.processInstId]}>
            {data.list.map(item => (
              <Panel header={item.processDisplayName} key={item.processInstId}>
                <HighTable
                  table={{
                    columns,
                    rowKey: 'taskInstId',
                    size: 'small',
                    dataSource: item.taskExecutorLogs,
                  }}
                />
              </Panel>
            ))}
          </Collapse>
          <Pagination
            size="small"
            pageSize={options.pageSize}
            current={options.pageNum}
            total={data.total}
            onChange={onChange}
            style={{ textAlign: 'right', marginTop: 10 }}
          />
        </>
      ) : (
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} />
      )}
    </Modal>
  );
};

export default Comp;
