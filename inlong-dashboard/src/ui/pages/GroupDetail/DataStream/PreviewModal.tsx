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
import { Modal, Table, Radio, RadioChangeEvent, Button, Card } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import i18n from '@/i18n';
import { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';

export interface Props extends ModalProps {
  inlongGroupId: string;
  inlongStreamId?: string;
  record?: Record<string, any>;
}

const Comp: React.FC<Props> = ({ inlongGroupId, inlongStreamId, ...modalProps }) => {
  const [position, setPosition] = useState(1);

  const [originalModal, setOriginalModal] = useState({
    open: false,
    record: {},
  });
  interface DataType {
    id: React.Key;
  }

  const onShowOriginModal = record => {
    setOriginalModal({ open: true, record: record });
  };

  const { data: previewData, run: getPreviewData } = useRequest(
    {
      url: '/stream/listMessages',
      params: {
        groupId: inlongGroupId,
        streamId: inlongStreamId,
        messageCount: position,
      },
    },
    {
      refreshDeps: [position],
      manual: inlongStreamId !== '' ? false : true,
    },
  );

  const getColumn = () => {
    let exitsId = false;
    const result = previewData?.[0]?.fieldList?.reduce((acc, cur) => {
      if (cur['fieldName'] === 'id') {
        exitsId = true;
      }
      const width =
        (cur['fieldName'].length > cur['fieldValue'].length
          ? cur['fieldName'].length
          : cur['fieldValue'].length) * 10;
      acc.push({
        title: cur['fieldName'],
        key: cur['fieldName'],
        dataIndex: cur['fieldName'],
        width: width >= 100 ? width : 100,
      });
      return acc;
    }, []);
    if (result) {
      if (exitsId) {
        return result;
      }
      return [{ title: 'id', key: 'id', dataIndex: 'id', width: 100 }].concat([...result]);
    }
    return;
  };

  const detailColumns: ColumnsType<DataType> = [
    {
      title: i18n.t('pages.GroupDetail.Stream.Dt'),
      key: 'dt',
      width: 200,
      dataIndex: 'dt',
    },
  ].concat(
    (getColumn() ? getColumn() : []).concat([
      {
        title: 'operation',
        key: 'operation',
        fixed: 'right',
        width: 100,
        render: (text, record: any) => (
          <>
            <Button
              type="link"
              onClick={() => {
                onShowOriginModal(record);
              }}
            >
              {i18n.t('pages.GroupDetail.Stream.ShowOriginal')}
            </Button>
          </>
        ),
      },
    ]),
  );
  const convertListToMap = () => {
    const result = [];
    for (let i = 0; i < previewData?.length; i++) {
      const temp = previewData?.[i]?.fieldList.reduce((acc, item) => {
        acc[item.fieldName] = item.fieldValue;
        return acc;
      }, {});
      temp['id'] = temp['id'] ? temp['id'] : i;
      temp['headers'] = previewData?.[i]?.headers;
      temp['body'] = previewData?.[i]?.body;
      temp['dt'] = dayjs(previewData?.[i]?.dt).format('YYYY-MM-DD HH:mm:ss');
      result.push(temp);
    }
    return result;
  };
  const onChange = ({ target: { value } }: RadioChangeEvent) => {
    setPosition(value);
  };
  useUpdateEffect(() => {
    if (modalProps.open) {
      if (inlongStreamId) {
        getPreviewData();
      }
    }
  }, [modalProps.open]);

  return (
    <Modal
      {...modalProps}
      title={i18n.t('pages.GroupDetail.Stream.Preview')}
      width={950}
      footer={null}
    >
      <div style={{ marginBottom: 20, marginTop: 20 }}>
        <span>{i18n.t('pages.GroupDetail.Stream.Number')}: </span>
        <Radio.Group defaultValue="1" style={{ marginLeft: 20 }} onChange={onChange}>
          <Radio.Button value="1">1</Radio.Button>
          <Radio.Button value="5">5</Radio.Button>
          <Radio.Button value="10">10</Radio.Button>
          <Radio.Button value="50">50</Radio.Button>
        </Radio.Group>
      </div>
      <Table
        columns={detailColumns}
        dataSource={convertListToMap()}
        scroll={{ x: 950 }}
        rowKey={'id'}
      ></Table>

      <Modal
        width={800}
        open={originalModal.open}
        title={i18n.t('pages.GroupDetail.Stream.Original')}
        onCancel={() => setOriginalModal(prev => ({ ...prev, open: false }))}
        footer={[
          <Button key="back" onClick={() => setOriginalModal(prev => ({ ...prev, open: false }))}>
            {i18n.t('pages.GroupDetail.Stream.Closed')}
          </Button>,
        ]}
      >
        <div>
          <Card title="headers" bordered={false} style={{ width: 700 }}>
            <p style={{ margin: 0 }}>{JSON.stringify(originalModal?.record['headers'])}</p>
          </Card>
          <Card title="body" bordered={false} style={{ width: 700, marginTop: 10 }}>
            <p style={{ margin: 0 }}>{originalModal?.record['body']}</p>
          </Card>
        </div>
      </Modal>
    </Modal>
  );
};

export default Comp;
