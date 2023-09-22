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

import React, { useEffect, useState } from 'react';
import { Button, Card, Col, Divider, Row, Table } from 'antd';
import { DoubleRightOutlined, PlusCircleOutlined } from '@ant-design/icons';
import SyncSources from '../SyncSources';
import SyncSink from '../SyncSink';
import SyncTransform from '../SyncTransform';
import EditableTable, { ColumnsItemProps } from '@/ui/components/EditableTable';
import i18n from '@/i18n';
import FieldList from '@/ui/components/FieldList';
import { ColumnsType } from 'antd/es/table';
import { useRequest } from 'ahooks';

export interface Props {
  inlongGroupId: string;
  inlongStreamId: string;
}

const Comp: React.FC<Props> = ({ inlongGroupId, inlongStreamId }) => {
  const [openT, setOpenT] = useState({ open: false });
  const openClick = () => {
    setOpenT({ open: openT.open === false ? true : false });
  };

  const { data, run: getList } = useRequest({
    url: '/transform/list',
    method: 'POST',
    data: {
      inlongGroupId,
      inlongStreamId,
      pageSize: 10,
      pageNum: 1,
    },
  });

  const sinkColumns: ColumnsType = [
    {
      title: i18n.t('components.FieldList.SinkFieldName'),
      dataIndex: 'fieldName',
    },
    {
      title: i18n.t('components.FieldList.SinkFieldType'),
      dataIndex: 'fieldType',
    },
    {
      title: i18n.t('components.FieldList.FieldComment'),
      dataIndex: 'fieldComment',
    },
  ];

  const sourceColumns: ColumnsType = [
    {
      title: i18n.t('meta.Stream.FieldName'),
      dataIndex: 'fieldName',
    },
    {
      title: i18n.t('meta.Stream.FieldType'),
      dataIndex: 'fieldType',
    },
    {
      title: i18n.t('meta.Stream.FieldComment'),
      dataIndex: 'fieldComment',
    },
  ];

  useEffect(() => {
    getList();
  }, [getList]);

  return (
    <>
      {openT.open || data?.total !== 0 ? (
        <Row gutter={[40, 48]}>
          <Col span={8}>
            <SyncSources inlongGroupId={inlongGroupId} inlongStreamId={inlongStreamId} />
          </Col>
          <DoubleRightOutlined
            style={{ position: 'absolute', top: '25%', left: 'calc(33% - 7px)' }}
          />
          <Col span={8} onDoubleClick={openClick}>
            <SyncTransform inlongGroupId={inlongGroupId} inlongStreamId={inlongStreamId} />
          </Col>
          <DoubleRightOutlined
            style={{ position: 'absolute', top: '25%', left: 'calc(67% - 7px)' }}
          />
          <Col span={8}>
            <SyncSink inlongGroupId={inlongGroupId} inlongStreamId={inlongStreamId} />
          </Col>
        </Row>
      ) : (
        <Row gutter={[40, 48]}>
          <Col span={11}>
            <SyncSources inlongGroupId={inlongGroupId} inlongStreamId={inlongStreamId} />
          </Col>
          <Col
            span={2}
            onDoubleClick={openClick}
            style={{
              position: 'relative',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <a
              type="link"
              style={{
                position: 'absolute',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              <Button type="link">{i18n.t('pages.SynchronizeDetail.Sync.Transform')}</Button>
            </a>
          </Col>
          <Col span={11}>
            <SyncSink inlongGroupId={inlongGroupId} inlongStreamId={inlongStreamId} />
          </Col>
        </Row>
      )}
      <Row style={{ marginTop: 50 }} gutter={[40, 48]}>
        <Col span={11}>
          <FieldList
            inlongGroupId={inlongGroupId}
            inlongStreamId={inlongStreamId}
            isSource={true}
            columns={sourceColumns}
          ></FieldList>
        </Col>
        <Col span={11} offset={2}>
          <FieldList
            inlongGroupId={inlongGroupId}
            inlongStreamId={inlongStreamId}
            isSource={false}
            columns={sinkColumns}
          ></FieldList>
        </Col>
      </Row>
    </>
  );
};

export default Comp;
