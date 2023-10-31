/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useState } from 'react';
import { Button, Form, Input, InputNumber, Space } from 'antd';
import DetailModal from './DetailModal';
import i18n from '@/i18n';

export interface Props {
  sinkType: string;
  fieldName: string;
  inlongGroupId?: string;
  inlongStreamId?: string;
  useNumber?: Boolean;
  sinkObj?: React.RefObject<any>;
}

const CreateTable: React.FC<Props> = ({
  sinkType,
  fieldName,
  inlongGroupId,
  inlongStreamId,
  useNumber,
  sinkObj,
}) => {
  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    open: false,
  });

  return (
    <Space>
      {useNumber ? (
        <>
          <Form.Item name={fieldName} rules={[{ required: true }]}>
            <InputNumber min={0} />
          </Form.Item>
        </>
      ) : (
        <>
          <Form.Item name={fieldName} rules={[{ required: true }]}>
            <Input style={{ width: 200 }} />
          </Form.Item>
        </>
      )}
      <Button
        style={{ marginBottom: 20 }}
        type="link"
        onClick={() => {
          setCreateModal({ open: true });
          console.log(sinkObj, 'cll');
        }}
      >
        {i18n.t('components.CreateTable.Table')}
      </Button>

      <DetailModal
        {...createModal}
        sinkType={sinkType}
        sinkObj={sinkObj}
        inlongGroupId={inlongGroupId}
        inlongStreamId={inlongStreamId}
        open={createModal.open as boolean}
        onOk={async () => {
          setCreateModal({ open: false });
        }}
        onCancel={() => setCreateModal({ open: false })}
      />
    </Space>
  );
};

export default CreateTable;
